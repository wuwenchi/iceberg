/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import org.jetbrains.annotations.Nullable;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.math.Ordering;

class SparkZOrderUDF implements Serializable {
  private static final byte[] PRIMITIVE_EMPTY = new byte[ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE];
  private final int numCols;
  private final int varTypeSize;
  private final int maxOutputSize;
  /**
   * Every Spark task runs iteratively on a rows in a single thread so ThreadLocal should protect from
   * concurrent access to any of these structures.
   */
  private transient ThreadLocal<ByteBuffer> outputBuffer;
  private transient ThreadLocal<byte[][]> inputHolder;
  private transient ThreadLocal<ByteBuffer[]> inputBuffers;
  private transient ThreadLocal<CharsetEncoder> encoder;
  private int inputCol = 0;
  private int totalOutputBytes = 0;
  private final UserDefinedFunction interleaveUDF =
      functions.udf((Seq<byte[]> arrayBinary) -> interleaveBits(arrayBinary), DataTypes.BinaryType)
          .withName("INTERLEAVE_BYTES");

  SparkZOrderUDF(int numCols, int varTypeSize, int maxOutputSize) {
    this.numCols = numCols;
    this.varTypeSize = varTypeSize;
    this.maxOutputSize = maxOutputSize;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    inputBuffers = ThreadLocal.withInitial(() -> new ByteBuffer[numCols]);
    inputHolder = ThreadLocal.withInitial(() -> new byte[numCols][]);
    outputBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(totalOutputBytes));
    encoder = ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newEncoder());
  }

  private ByteBuffer inputBuffer(int position, int size) {
    ByteBuffer buffer = inputBuffers.get()[position];
    if (buffer == null) {
      buffer = ByteBuffer.allocate(size);
      inputBuffers.get()[position] = buffer;
    }
    return buffer;
  }

  byte[] interleaveBits(Seq<byte[]> scalaBinary) {
    byte[][] columnsBinary = JavaConverters.seqAsJavaList(scalaBinary).toArray(inputHolder.get());
    return ZOrderByteUtils.interleaveBits(columnsBinary, totalOutputBytes, outputBuffer.get());
  }

  private UserDefinedFunction tinyToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((Byte value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.tinyintToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
          .array();
    }, DataTypes.BinaryType).withName("TINY_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction shortToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((Short value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.shortToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
          .array();
    }, DataTypes.BinaryType).withName("SHORT_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction intToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((Integer value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
          .array();
    }, DataTypes.BinaryType).withName("INT_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction longToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((Long value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.longToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
          .array();
    }, DataTypes.BinaryType).withName("LONG_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }


  /**
   * TODO xzw
   */
  private <K> UserDefinedFunction longToOrderedBytesUDF(Supplier<K> supplier) {
    UserDefinedFunction udf = functions
        .udf(supplier.get(), DataTypes.BinaryType).withName("LONG_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);
    return udf;
  }

  /**
   * TODO xzw
   */
  private <K> int getBound(Object key, K[] candidateBounds, BiFunction<Object, K, Boolean> bif) {
    int bound = 0;
    if (candidateBounds.length <= 128) {
      while (bound < candidateBounds.length && bif.apply(key, candidateBounds[bound])) {
        bound += 1;
      }
    } else {
      bound = binarySearch(candidateBounds, key);
      if (bound < 0) {
        bound = -bound - 1;
      }
      if (bound > candidateBounds.length) {
        bound = candidateBounds.length;
      }
    }
    return bound;
  }

  /**
   * TODO xzw
   */
  private <K> int binarySearch(K[] v1, K key) {
    return Arrays.binarySearch(v1, key);
  }

  private UserDefinedFunction floatToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((Float value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.floatToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
          .array();
    }, DataTypes.BinaryType).withName("FLOAT_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction doubleToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((Double value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.doubleToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
          .array();
    }, DataTypes.BinaryType).withName("DOUBLE_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  private UserDefinedFunction booleanToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((Boolean value) -> {
      ByteBuffer buffer = inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);
      buffer.put(0, (byte) (value ? -127 : 0));
      return buffer.array();
    }, DataTypes.BinaryType).withName("BOOLEAN-LEXICAL-BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);
    return udf;
  }

  private UserDefinedFunction stringToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((String value) ->
            ZOrderByteUtils.stringToOrderedBytes(
                value,
                varTypeSize,
                inputBuffer(position, varTypeSize),
                encoder.get()).array(), DataTypes.BinaryType)
        .withName("STRING-LEXICAL-BYTES");

    this.inputCol++;
    increaseOutputSize(varTypeSize);

    return udf;
  }

  /**
   * TODO xzw
   */
  private <K> UserDefinedFunction stringToOrderedBytesUDF(K[] candidateBounds) {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((String value) ->
            ZOrderByteUtils
                .intToOrderedBytes(
                    getBound(value, candidateBounds, (k, k2) -> Ordering.String.gt(k, k2)),
                    inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
                .array(), DataTypes.BinaryType)
        .withName("STRING-LEXICAL-BYTES");

    this.inputCol++;
    increaseOutputSize(varTypeSize);

    return udf;
  }

  private UserDefinedFunction bytesTruncateUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((byte[] value) ->
                ZOrderByteUtils.byteTruncateOrFill(value, varTypeSize, inputBuffer(position, varTypeSize)).array(),
            DataTypes.BinaryType)
        .withName("BYTE-TRUNCATE");

    this.inputCol++;
    increaseOutputSize(varTypeSize);

    return udf;
  }

  Column interleaveBytes(Column arrayBinary) {
    return interleaveUDF.apply(arrayBinary);
  }

  /**
   * TODO xzw
   */
  public <K> Column sortedNew(Column column, DataType type, K[] candidateBounds) {
    int position = inputCol;

    if (type instanceof LongType) {
      Supplier<UDF1<Long, Object>> supplier = () -> (Long value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils.intToOrderedBytes(getBound(value, candidateBounds,
                    (k, k2) -> Ordering.Long.gt(k, k2)),
                inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
            .array();
      };
      return longToOrderedBytesUDF(supplier).apply(column);

    } else if (type instanceof DoubleType || type instanceof FloatType) {
      Supplier<UDF1<Double, Object>> supplier = () -> (Double value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils.intToOrderedBytes(getBound(Double.doubleToLongBits(value), candidateBounds,
                    (k, k2) -> Ordering.Long.gt(k, k2)),
                inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
            .array();
      };
      return longToOrderedBytesUDF(supplier).apply(column);

    } else if (type instanceof IntegerType || type instanceof ShortType) {
      Supplier<UDF1<Integer, Object>> supplier = () -> (Integer value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils.intToOrderedBytes(getBound(value.longValue(), candidateBounds,
                    (k, k2) -> Ordering.Long.gt(k, k2)),
                inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
            .array();
      };
      return longToOrderedBytesUDF(supplier).apply(column);

    } else if (type instanceof StringType) {
      Supplier<UDF1<String, Object>> supplier = () -> (String value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils.intToOrderedBytes(getBound(value, candidateBounds,
                    (k, k2) -> Ordering.Long.gt(k, k2)),
                inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
            .array();
      };
      return longToOrderedBytesUDF(supplier).apply(column);

    } else if (type instanceof DateType) {
      Supplier<UDF1<Date, Object>> supplier = () -> (Date value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils.intToOrderedBytes(getBound(value.getTime(), candidateBounds,
                    (k, k2) -> Ordering.Long.gt(k, k2)),
                inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
            .array();
      };
      return longToOrderedBytesUDF(supplier).apply(column);

    } else if (type instanceof TimestampType) {
      Supplier<UDF1<Timestamp, Object>> supplier = () -> (Timestamp value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils.intToOrderedBytes(getBound(value.getTime(), candidateBounds,
                    (k, k2) -> Ordering.Long.gt(k, k2)),
                inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
            .array();
      };
      return longToOrderedBytesUDF(supplier).apply(column);

    } else if (type instanceof ByteType) {
      Supplier<UDF1<Byte, Object>> supplier = () -> (Byte value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils.intToOrderedBytes(getBound((int) value, candidateBounds,
                    (k, k2) -> Ordering.Long.gt(k, k2)),
                inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
            .array();
      };
      return longToOrderedBytesUDF(supplier).apply(column);

    } else if (type instanceof DecimalType) {
      Supplier<UDF1<Decimal, Object>> supplier = () -> (Decimal value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils.intToOrderedBytes(getBound(value.toLong(), candidateBounds,
                    (k, k2) -> Ordering.Long.gt(k, k2)),
                inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
            .array();
      };
      return longToOrderedBytesUDF(supplier).apply(column);

    }
    return null;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  Column sortedLexicographically(Column column, DataType type) {
    if (type instanceof ByteType) {
      return tinyToOrderedBytesUDF().apply(column);
    } else if (type instanceof ShortType) {
      return shortToOrderedBytesUDF().apply(column);
    } else if (type instanceof IntegerType) {
      return intToOrderedBytesUDF().apply(column);
    } else if (type instanceof LongType) {
      return longToOrderedBytesUDF().apply(column);
    } else if (type instanceof FloatType) {
      return floatToOrderedBytesUDF().apply(column);
    } else if (type instanceof DoubleType) {
      return doubleToOrderedBytesUDF().apply(column);
    } else if (type instanceof StringType) {
      return stringToOrderedBytesUDF().apply(column);
    } else if (type instanceof BinaryType) {
      return bytesTruncateUDF().apply(column);
    } else if (type instanceof BooleanType) {
      return booleanToOrderedBytesUDF().apply(column);
    } else if (type instanceof TimestampType) {
      return longToOrderedBytesUDF().apply(column.cast(DataTypes.LongType));
    } else if (type instanceof DateType) {
      return longToOrderedBytesUDF().apply(column.cast(DataTypes.LongType));
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot use column %s of type %s in ZOrdering, the type is unsupported", column, type));
    }
  }

  private void increaseOutputSize(int bytes) {
    totalOutputBytes = Math.min(totalOutputBytes + bytes, maxOutputSize);
  }
}
