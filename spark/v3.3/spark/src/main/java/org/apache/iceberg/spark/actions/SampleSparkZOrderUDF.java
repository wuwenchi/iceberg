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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

import static org.apache.iceberg.spark.actions.SparkZOrderUDFUtils.getLongBound;
import static org.apache.iceberg.spark.actions.SparkZOrderUDFUtils.getStringBound;
import static org.apache.iceberg.util.ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;
import static org.apache.iceberg.util.ZOrderByteUtils.longToOrderedBytes;

class SampleSparkZOrderUDF extends BaseSparkZOrderUDF {

  SampleSparkZOrderUDF(int numCols, int maxOutputSize) {
    super(numCols, maxOutputSize);

  }

  @Override
  Column applyToBytesUdf(Column column, DataType type, Object candidateBounds) {
    if (type instanceof DecimalType) {
      return new DecimalToBytesZOrderUDFProvider().getUDF(candidateBounds).apply(column);
    } else if (supportType(type)) {
      return udfMap.get(type.typeName()).getUDF(candidateBounds).apply(column);
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot use column %s of type %s in ZOrdering, the type is unsupported", column, type));
    }
  }

  @Override
  void init() {
    udfMap.put(DataTypes.LongType.typeName(), new LongToBytesZOrderUDFProvider());
    udfMap.put(DataTypes.DoubleType.typeName(), new DoubleToBytesZOrderUDFProvider());
    udfMap.put(DataTypes.FloatType.typeName(), new FloatToBytesZOrderUDFProvider());
    udfMap.put(DataTypes.IntegerType.typeName(), new IntegerToBytesZOrderUDFProvider());
    udfMap.put(DataTypes.ShortType.typeName(), new ShortToBytesZOrderUDFProvider());
    udfMap.put(DataTypes.StringType.typeName(), new StringToBytesZOrderUDFProvider());
    udfMap.put(DataTypes.DateType.typeName(), new DateToBytesZOrderUDFProvider());
    udfMap.put(DataTypes.TimestampType.typeName(), new TimestampToBytesZOrderUDFProvider());
    udfMap.put(DataTypes.ByteType.typeName(), new ByteToBytesZOrderUDFProvider());
    supportedTypes = ImmutableSet.<String>builder()
        .addAll(udfMap.keySet()).build();
  }

  private byte[] boundToBytes(int bound, int position) {
    return longToOrderedBytes(bound, inputBuffer(position, PRIMITIVE_BUFFER_SIZE)).array();
  }

  private class LongToBytesZOrderUDFProvider implements SparkZOrderUDFProvider {
    @Override
    public UserDefinedFunction getUDF(Object candidateBounds) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Long value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return boundToBytes(getLongBound(value, (long[]) candidateBounds), position);
      }, DataTypes.BinaryType);

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  private class DoubleToBytesZOrderUDFProvider implements SparkZOrderUDFProvider {
    @Override
    public UserDefinedFunction getUDF(Object candidateBounds) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Double value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return boundToBytes(getLongBound(Double.doubleToLongBits(value), (long[]) candidateBounds), position);
      }, DataTypes.BinaryType);

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  private class FloatToBytesZOrderUDFProvider implements SparkZOrderUDFProvider {
    @Override
    public UserDefinedFunction getUDF(Object candidateBounds) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Float value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return boundToBytes(getLongBound(Double.doubleToLongBits(value), (long[]) candidateBounds), position);
      }, DataTypes.BinaryType);
      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  private class IntegerToBytesZOrderUDFProvider implements SparkZOrderUDFProvider {
    @Override
    public UserDefinedFunction getUDF(Object candidateBounds) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Integer value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return boundToBytes(getLongBound(value.longValue(), (long[]) candidateBounds), position);
      }, DataTypes.BinaryType);

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  private class ShortToBytesZOrderUDFProvider implements SparkZOrderUDFProvider {
    @Override
    public UserDefinedFunction getUDF(Object candidateBounds) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Short value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return boundToBytes(getLongBound(value.longValue(), (long[]) candidateBounds), position);
      }, DataTypes.BinaryType);

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  private class StringToBytesZOrderUDFProvider implements SparkZOrderUDFProvider {
    @Override
    public UserDefinedFunction getUDF(Object candidateBounds) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf(
          (String value) -> {
            if (value == null) {
              return PRIMITIVE_EMPTY;
            }
            return boundToBytes(getStringBound(value, (String[]) candidateBounds), position);
          }, DataTypes.BinaryType);

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  private class DateToBytesZOrderUDFProvider implements SparkZOrderUDFProvider {
    @Override
    public UserDefinedFunction getUDF(Object candidateBounds) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Date value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return boundToBytes(getLongBound(value.getTime(), (long[]) candidateBounds), position);
      }, DataTypes.BinaryType);

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  private class TimestampToBytesZOrderUDFProvider implements SparkZOrderUDFProvider {
    @Override
    public UserDefinedFunction getUDF(Object candidateBounds) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((Timestamp value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return boundToBytes(getLongBound(value.getTime(), (long[]) candidateBounds), position);
      }, DataTypes.BinaryType);

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  private class ByteToBytesZOrderUDFProvider implements SparkZOrderUDFProvider {
    @Override
    public UserDefinedFunction getUDF(Object candidateBounds) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf(
          (Byte value) -> {
            if (value == null) {
              return PRIMITIVE_EMPTY;
            }
            return boundToBytes(getLongBound((long) value, (long[]) candidateBounds), position);
          }, DataTypes.BinaryType);

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }

  private class DecimalToBytesZOrderUDFProvider implements SparkZOrderUDFProvider {
    @Override
    public UserDefinedFunction getUDF(Object candidateBounds) {
      int position = inputCol;
      UserDefinedFunction udf = functions.udf((BigDecimal value) -> {
        if (value == null) {
          return PRIMITIVE_EMPTY;
        }
        return boundToBytes(getLongBound(value.longValue(), (long[]) candidateBounds), position);
      }, DataTypes.BinaryType);

      increase(PRIMITIVE_BUFFER_SIZE);
      return udf;
    }
  }
}
