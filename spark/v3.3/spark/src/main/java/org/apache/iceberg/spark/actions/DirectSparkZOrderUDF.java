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

import java.nio.ByteBuffer;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.TimestampType;

import static org.apache.iceberg.util.ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;

class DirectSparkZOrderUDF extends BaseSparkZOrderUDF {

  private final int varTypeSize;

  DirectSparkZOrderUDF(int numCols, int varTypeSize, int maxOutputSize) {
    super(numCols, maxOutputSize);
    this.varTypeSize = varTypeSize;
  }

  @Override
  Column applyToBytesUdf(Column column, DataType type, Object obj) {
    if (type instanceof TimestampType) {
      return udfMap.get(DataTypes.LongType.typeName()).getUDF(obj).apply(column.cast(DataTypes.LongType));
    } else if (supportType(type)) {
      udfMap.get(type.typeName()).getUDF(obj).apply(column);
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot use column %s of type %s in ZOrdering, the type is unsupported", column, type));
    }
    return null;
  }

  @Override
  void init() {
    udfMap.put(DataTypes.ByteType.typeName(), new TinyToOrderedBytesZOrderUDFProvider());
    udfMap.put(DataTypes.ShortType.typeName(), new ShortToOrderedBytesZOrderUDFProvider());
    udfMap.put(DataTypes.IntegerType.typeName(), new IntToOrderedBytesZOrderUDFProvider());
    udfMap.put(DataTypes.LongType.typeName(), new IntToOrderedBytesZOrderUDFProvider());
    udfMap.put(DataTypes.FloatType.typeName(), new FloatToOrderedBytesZOrderUDFProvider());
    udfMap.put(DataTypes.DoubleType.typeName(), new DoubleToOrderedBytesZOrderUDFProvider());
    udfMap.put(DataTypes.StringType.typeName(), new StringToOrderedBytesZOrderUDFProvider());
    udfMap.put(DataTypes.BinaryType.typeName(), new BytesTruncateZOrderUDFProvider());
    udfMap.put(DataTypes.BooleanType.typeName(), new BooleanToOrderedBytesZOrderUDFProvider());
    supportedTypes = ImmutableSet.<String>builder().addAll(udfMap.keySet()).build();
  }

  class TinyToOrderedBytesZOrderUDFProvider implements SparkZOrderUDFProvider {

    @Override
    public UserDefinedFunction getUDF(Object obj) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Byte value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils
            .tinyintToOrderedBytes(value, inputBuffer(position, PRIMITIVE_BUFFER_SIZE))
            .array();
      }, DataTypes.BinaryType).withName("TINY_ORDERED_BYTES");

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  class ShortToOrderedBytesZOrderUDFProvider implements SparkZOrderUDFProvider {

    @Override
    public UserDefinedFunction getUDF(Object obj) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Short value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils
            .shortToOrderedBytes(value, inputBuffer(position, PRIMITIVE_BUFFER_SIZE))
            .array();
      }, DataTypes.BinaryType).withName("SHORT_ORDERED_BYTES");

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  class IntToOrderedBytesZOrderUDFProvider implements SparkZOrderUDFProvider {

    @Override
    public UserDefinedFunction getUDF(Object obj) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Integer value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils
            .intToOrderedBytes(value, inputBuffer(position, PRIMITIVE_BUFFER_SIZE))
            .array();
      }, DataTypes.BinaryType).withName("INT_ORDERED_BYTES");

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  class LongToOrderedBytesZOrderUDFProvider implements SparkZOrderUDFProvider {

    @Override
    public UserDefinedFunction getUDF(Object obj) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Long value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils
            .longToOrderedBytes(value, inputBuffer(position, PRIMITIVE_BUFFER_SIZE))
            .array();
      }, DataTypes.BinaryType).withName("LONG_ORDERED_BYTES");

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  class FloatToOrderedBytesZOrderUDFProvider implements SparkZOrderUDFProvider {

    @Override
    public UserDefinedFunction getUDF(Object obj) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Float value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils
            .floatToOrderedBytes(value, inputBuffer(position, PRIMITIVE_BUFFER_SIZE))
            .array();
      }, DataTypes.BinaryType).withName("FLOAT_ORDERED_BYTES");

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  class DoubleToOrderedBytesZOrderUDFProvider implements SparkZOrderUDFProvider {

    @Override
    public UserDefinedFunction getUDF(Object obj) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Double value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return ZOrderByteUtils
            .doubleToOrderedBytes(value, inputBuffer(position, PRIMITIVE_BUFFER_SIZE))
            .array();
      }, DataTypes.BinaryType).withName("DOUBLE_ORDERED_BYTES");

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  class BooleanToOrderedBytesZOrderUDFProvider implements SparkZOrderUDFProvider {

    @Override
    public UserDefinedFunction getUDF(Object obj) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Boolean value) -> {
        ByteBuffer buffer = inputBuffer(position, PRIMITIVE_BUFFER_SIZE);
        buffer.put(0, (byte) (value ? -127 : 0));
        return buffer.array();
      }, DataTypes.BinaryType).withName("BOOLEAN-LEXICAL-BYTES");

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  class StringToOrderedBytesZOrderUDFProvider implements SparkZOrderUDFProvider {

    @Override
    public UserDefinedFunction getUDF(Object obj) {
      int position = inputCol;
      UserDefinedFunction udf = functions
          .udf(
              (String value) -> ZOrderByteUtils.stringToOrderedBytes(value,
                  varTypeSize, inputBuffer(position, varTypeSize), encoder.get()).array(),
              DataTypes.BinaryType)
          .withName("STRING-LEXICAL-BYTES");

      increase(varTypeSize);
      return udf;
    }
  }

  class BytesTruncateZOrderUDFProvider implements SparkZOrderUDFProvider {

    @Override
    public UserDefinedFunction getUDF(Object obj) {
      int position = inputCol;
      UserDefinedFunction udf = functions
          .udf(
              (byte[] value) -> ZOrderByteUtils.byteTruncateOrFill(value,
                  varTypeSize, inputBuffer(position, varTypeSize)).array(),
              DataTypes.BinaryType)
          .withName("BYTE-TRUNCATE");

      increase(varTypeSize);
      return udf;
    }
  }
}
