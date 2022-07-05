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

import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.api.java.UDF1;
import scala.math.Ordering;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.function.BiFunction;

import static org.apache.iceberg.spark.actions.SparkZOrderUDF.PRIMITIVE_EMPTY;

class SparkZOrderUDFUtils {

  static <K> UDF1<Long, Object> longToBytesUDF(ByteBuffer buffer, K[] candidateBounds) {
    return (Long value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(getBound(value, candidateBounds,
              (k, k2) -> Ordering.Long.gt(k, k2)), buffer)
          .array();
    };
  }

  static <K> UDF1<Double, Object> doubleToBytesUDF(ByteBuffer buffer, K[] candidateBounds) {
    return (Double value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(getBound(Double.doubleToLongBits(value), candidateBounds,
              (k, k2) -> Ordering.Long.gt(k, k2)), buffer)
          .array();
    };
  }

  static <K> UDF1<Float, Object> floatToBytesUDF(ByteBuffer buffer, K[] candidateBounds) {
    return (Float value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(getBound(Double.doubleToLongBits(value), candidateBounds,
              (k, k2) -> Ordering.Long.gt(k, k2)), buffer)
          .array();
    };
  }

  static <K> UDF1<Integer, Object> integerToBytesUDF(ByteBuffer buffer, K[] candidateBounds) {
    return (Integer value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(getBound(value.longValue(), candidateBounds,
              (k, k2) -> Ordering.Long.gt(k, k2)), buffer)
          .array();
    };
  }

  static <K> UDF1<Short, Object> shortToBytesUDF(ByteBuffer buffer, K[] candidateBounds) {
    return (Short value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(getBound(value.longValue(), candidateBounds,
              (k, k2) -> Ordering.Long.gt(k, k2)), buffer)
          .array();
    };
  }

  static <K> UDF1<String, Object> stringToBytesUDF(ByteBuffer buffer, K[] candidateBounds) {
    return (String value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(getBound(value, candidateBounds,
              (k, k2) -> Ordering.Long.gt(k, k2)), buffer)
          .array();
    };
  }

  static <K> UDF1<Date, Object> dateToBytesUDF(ByteBuffer buffer, K[] candidateBounds) {
    return (Date value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(getBound(value.getTime(), candidateBounds,
              (k, k2) -> Ordering.Long.gt(k, k2)), buffer)
          .array();
    };
  }

  static <K> UDF1<Timestamp, Object> timestampToBytesUDF(ByteBuffer buffer, K[] candidateBounds) {
    return (Timestamp value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(getBound(value.getTime(), candidateBounds,
              (k, k2) -> Ordering.Long.gt(k, k2)), buffer)
          .array();
    };
  }

  static <K> UDF1<Byte, Object> byteToBytesUDF(ByteBuffer buffer, K[] candidateBounds) {
    return (Byte value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(getBound((int) value, candidateBounds,
              (k, k2) -> Ordering.Long.gt(k, k2)), buffer)
          .array();
    };
  }

  static <K> UDF1<BigDecimal, Object> decimalToBytesUDF(ByteBuffer buffer, K[] candidateBounds) {
    return (BigDecimal value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return ZOrderByteUtils.intToOrderedBytes(getBound(value.longValue(), candidateBounds,
              (k, k2) -> Ordering.Long.gt(k, k2)), buffer)
          .array();
    };
  }


  /**
   * TODO xzw
   */
  private static <K> int getBound(Object key, K[] candidateBounds, BiFunction<Object, K, Boolean> bif) {
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

  private static <K> int binarySearch(K[] v1, K key) {
    return Arrays.binarySearch(v1, key);
  }
}
