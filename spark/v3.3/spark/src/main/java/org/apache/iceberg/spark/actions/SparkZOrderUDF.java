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
import java.util.function.Supplier;

import org.apache.hadoop.shaded.javax.activation.UnsupportedDataTypeException;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.apache.iceberg.spark.actions.SparkZOrderUDFUtils.*;

class SparkZOrderUDF implements Serializable {
  static final byte[] PRIMITIVE_EMPTY = new byte[ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE];
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
  private final UserDefinedFunction interleaveUDF = functions.udf((Seq<byte[]> arrayBinary) -> interleaveBits(arrayBinary), DataTypes.BinaryType).withName("INTERLEAVE_BYTES");

  SparkZOrderUDF(int numCols, int varTypeSize, int maxOutputSize) {
    this.numCols = numCols;
    this.varTypeSize = varTypeSize;
    this.maxOutputSize = maxOutputSize;

    // TODO 为了测试用例而调用里面注释了一行代码
    try {
      readObject(null);
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
   // in.defaultReadObject();
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
      return ZOrderByteUtils.tinyintToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE)).array();
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
      return ZOrderByteUtils.shortToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE)).array();
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
      return ZOrderByteUtils.intToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE)).array();
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
      return ZOrderByteUtils.longToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE)).array();
    }, DataTypes.BinaryType).withName("LONG_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    return udf;
  }

  /**
   * TODO xzw
   */
  private <K> UserDefinedFunction toBytesUDF(Supplier<UDF1> supplier) {
    UserDefinedFunction udf = functions.udf(supplier.get(), DataTypes.BinaryType).withName("LONG_ORDERED_BYTES");

    this.inputCol++;
    increaseOutputSize(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);
    return udf;
  }

  private UserDefinedFunction floatToOrderedBytesUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((Float value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.floatToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE)).array();
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
      return ZOrderByteUtils.doubleToOrderedBytes(value, inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE)).array();
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
    UserDefinedFunction udf = functions.udf((String value) -> ZOrderByteUtils.stringToOrderedBytes(value, varTypeSize, inputBuffer(position, varTypeSize), encoder.get()).array(), DataTypes.BinaryType).withName("STRING-LEXICAL-BYTES");

    this.inputCol++;
    increaseOutputSize(varTypeSize);

    return udf;
  }

  private UserDefinedFunction bytesTruncateUDF() {
    int position = inputCol;
    UserDefinedFunction udf = functions.udf((byte[] value) -> ZOrderByteUtils.byteTruncateOrFill(value, varTypeSize, inputBuffer(position, varTypeSize)).array(), DataTypes.BinaryType).withName("BYTE-TRUNCATE");

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
  public  Column sortedNew(Column column, DataType type, Object[] candidateBounds) throws UnsupportedDataTypeException {
    int position = inputCol;
    ByteBuffer buffer = inputBuffer(position, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);

    if (type instanceof LongType) {
      return toBytesUDF(() -> longToBytesUDF(buffer, candidateBounds)).apply(column);
    } else if (type instanceof DoubleType) {
      return toBytesUDF(() -> doubleToBytesUDF(buffer, candidateBounds)).apply(column);
    } else if (type instanceof FloatType) {
      return toBytesUDF(() -> floatToBytesUDF(buffer, candidateBounds)).apply(column);
    } else if (type instanceof IntegerType) {
      return toBytesUDF(() -> integerToBytesUDF(buffer, candidateBounds)).apply(column);
    } else if (type instanceof ShortType) {
      return toBytesUDF(() -> shortToBytesUDF(buffer, candidateBounds)).apply(column);
    } else if (type instanceof StringType) {
      return toBytesUDF(() -> stringToBytesUDF(buffer, candidateBounds)).apply(column);
    } else if (type instanceof DateType) {
      return toBytesUDF(() -> dateToBytesUDF(buffer, candidateBounds)).apply(column);
    } else if (type instanceof TimestampType) {
      return toBytesUDF(() -> timestampToBytesUDF(buffer, candidateBounds)).apply(column);
    } else if (type instanceof ByteType) {
      return toBytesUDF(() -> byteToBytesUDF(buffer, candidateBounds)).apply(column);
    } else if (type instanceof DecimalType) {
      return toBytesUDF(() -> decimalToBytesUDF(buffer, candidateBounds)).apply(column);
    }else if(type instanceof BinaryType || type instanceof BooleanType){
      throw new UnsupportedDataTypeException(String.format("我现在不打算不支持这个类型%s",type));
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
      throw new IllegalArgumentException(String.format("Cannot use column %s of type %s in ZOrdering, the type is unsupported", column, type));
    }
  }

  private void increaseOutputSize(int bytes) {
    totalOutputBytes = Math.min(totalOutputBytes + bytes, maxOutputSize);
  }
}
