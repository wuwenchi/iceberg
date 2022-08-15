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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.util.ZOrderByteUtils;

public class ZOrderOptions {

  public static final String SPATIAL_CURVE_STRATEGY_TYPE_KEY = "spatial-curve-strategy-type";
  public static final String DEFAULT_SPATIAL_CURVE_STRATEGY_TYPE = "direct";
  public static final String BUILD_RANGE_SAMPLE_SIZE_KEY = "build_range_sample_size";
  public static final int DEFAULT_BUILD_RANGE_SAMPLE_SIZE = 100000;
  public static final String SAMPLE_POINTS_PER_PARTITION_HINT_KEY = "sample-points-per-partition-hint";

  /**
   * Controls the amount of bytes interleaved in the ZOrder Algorithm. Default is all bytes being interleaved.
   */
  public static final String MAX_OUTPUT_SIZE_KEY = "max-output-size";
  public static final int DEFAULT_MAX_OUTPUT_SIZE = Integer.MAX_VALUE;
  /**
   * Controls the number of bytes considered from an input column of a type with variable length (String, Binary).
   * Default is to use the same size as primitives {@link ZOrderByteUtils#PRIMITIVE_BUFFER_SIZE}
   */
  public static final String VAR_LENGTH_CONTRIBUTION_KEY = "var-length-contribution";
  public static final int DEFAULT_VAR_LENGTH_CONTRIBUTION = ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;
  public static final int DEFAULT_SAMPLE_POINTS_PER_PARTITION_HINT = 20;

  enum SpatialCurveStrategyType {
    /**
     * Direct value mapping
     */
    DIRECT("direct"),
    SAMPLE("sample");

    private static final Map<String, SpatialCurveStrategyType> TYPE_VALUE_MAP;

    static {
      TYPE_VALUE_MAP = Arrays
          .stream(SpatialCurveStrategyType.class.getEnumConstants())
          .collect(Collectors.toMap(e -> e.value, Function.identity()));
    }

    private final String value;

    SpatialCurveStrategyType(String value) {
      this.value = value;
    }

    public static SpatialCurveStrategyType fromValue(String type) {
      return TYPE_VALUE_MAP.get(type);
    }

    public String getValue() {
      return value;
    }
  }
}
