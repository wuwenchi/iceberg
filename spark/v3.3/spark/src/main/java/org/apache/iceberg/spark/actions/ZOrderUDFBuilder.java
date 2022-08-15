/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.actions;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import static org.apache.iceberg.spark.actions.ZOrderOptions.DEFAULT_SPATIAL_CURVE_STRATEGY_TYPE;
import static org.apache.iceberg.spark.actions.ZOrderOptions.SPATIAL_CURVE_STRATEGY_TYPE_KEY;

public class ZOrderUDFBuilder {
  List<StructField> zOrderColumns;
  Dataset<Row> scanDF;
  Map<String, String> options;
  SparkSession spark;
  private int numCols;

  public ZOrderUDFBuilder(SparkSession spark) {
    this.spark = spark;
  }

  public ZOrderUDFBuilder setZOrderColumns(List<StructField> zOrderColumns) {
    this.zOrderColumns = zOrderColumns;
    return this;
  }

  public ZOrderUDFBuilder setScanDF(Dataset<Row> scanDF) {
    this.scanDF = scanDF;
    return this;
  }

  public ZOrderUDFBuilder setOptions(Map<String, String> options) {
    this.options = options;
    return this;
  }

  public ZOrderUDFBuilder setNumCols(int numCols) {
    this.numCols = numCols;
    return this;
  }

  public ZOrderUDFBuilder setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }

  public BaseSparkZOrderUDF build() {
    String type = PropertyUtil.propertyAsString(
        options,
        SPATIAL_CURVE_STRATEGY_TYPE_KEY,
        DEFAULT_SPATIAL_CURVE_STRATEGY_TYPE);
    ZOrderOptions.SpatialCurveStrategyType spatialCurveStrategyType =
        ZOrderOptions.SpatialCurveStrategyType.fromValue(type);
    Preconditions.checkArgument(spatialCurveStrategyType != null,
        "Unsupported type with zOrder, %s was set to %s", SPATIAL_CURVE_STRATEGY_TYPE_KEY, type);

    switch (spatialCurveStrategyType) {
      case DIRECT:
        return new DirectSparkZOrderUDF(numCols, zOrderColumns, options, spark);
      case SAMPLE:
        return new SampleSparkZOrderUDF(numCols, zOrderColumns, scanDF, options, spark);
      default:
        throw new UnsupportedOperationException(String.format("Unsupported type %s", spatialCurveStrategyType));
    }
  }
}
