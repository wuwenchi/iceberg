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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.RangeSampleSort$;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;

import static org.apache.iceberg.spark.actions.SparkZOrderUDFUtils.getLongBound;
import static org.apache.iceberg.spark.actions.SparkZOrderUDFUtils.getStringBound;
import static org.apache.iceberg.spark.actions.ZOrderOptions.BUILD_RANGE_SAMPLE_SIZE_KEY;
import static org.apache.iceberg.spark.actions.ZOrderOptions.DEFAULT_BUILD_RANGE_SAMPLE_SIZE;
import static org.apache.iceberg.spark.actions.ZOrderOptions.DEFAULT_SAMPLE_POINTS_PER_PARTITION_HINT;
import static org.apache.iceberg.spark.actions.ZOrderOptions.SAMPLE_POINTS_PER_PARTITION_HINT_KEY;
import static org.apache.iceberg.util.ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;
import static org.apache.iceberg.util.ZOrderByteUtils.longToOrderedBytes;

class SampleSparkZOrderUDF extends BaseSparkZOrderUDF {
  private final DecimalToBytesZOrderUDFProvider decimalToBytesZOrderUDFProvider = new DecimalToBytesZOrderUDFProvider();
  int samplePointsPerPartitionHint;
  int buildRangeSampleSize;
  private Dataset<Row> scanDF;

  SampleSparkZOrderUDF(
      int numCols,
      List<StructField> zOrderColumns, Dataset<Row> scanDF, Map<String, String> options, SparkSession spark) {
    super(numCols, zOrderColumns, options, spark);
    this.scanDF = scanDF;
  }


  @Override
  void init() {

    samplePointsPerPartitionHint = PropertyUtil.propertyAsInt(
        getOptions(),
        SAMPLE_POINTS_PER_PARTITION_HINT_KEY,
        DEFAULT_SAMPLE_POINTS_PER_PARTITION_HINT);
    Preconditions.checkArgument(samplePointsPerPartitionHint > 0,
        "Cannot have ZOrder use a partition hint that is less than 1, %s was set to %s",
        SAMPLE_POINTS_PER_PARTITION_HINT_KEY, samplePointsPerPartitionHint);

    buildRangeSampleSize = PropertyUtil.propertyAsInt(
        getOptions(),
        BUILD_RANGE_SAMPLE_SIZE_KEY,
        DEFAULT_BUILD_RANGE_SAMPLE_SIZE);
    Preconditions.checkArgument(buildRangeSampleSize > 0,
        "Cannot use less than 1 for range sample size with zOrder, %s was set to %s",
        BUILD_RANGE_SAMPLE_SIZE_KEY, buildRangeSampleSize);

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

  @Override
  Column compute() {
    Object[] rangeBound = RangeSampleSort$.MODULE$.getRangeBound(
        scanDF,
        JavaConverters.asScalaBuffer(zOrderColumns),
        buildRangeSampleSize,
        samplePointsPerPartitionHint);

    Broadcast<Object[]> broadcast = spark().sparkContext().broadcast(rangeBound, ClassTag.apply(Object[].class));

    Column[] columns = new Column[zOrderColumns.size()];
    for (int i = 0; i < zOrderColumns.size(); i++) {
      StructField colStruct = zOrderColumns.get(i);

      Column column = functions.col(colStruct.name());
      DataType type = colStruct.dataType();
      if (type instanceof DecimalType) {
        columns[i] = decimalToBytesZOrderUDFProvider.getUDF(broadcast.value()[i]).apply(column);
      } else {
        columns[i] = toBytes(type, broadcast.value()[i], column);
      }
    }
    return functions.array(columns);
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
