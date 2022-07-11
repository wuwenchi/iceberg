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

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.function.BiFunction;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.iceberg.spark.actions.SparkZOrderUDF.PRIMITIVE_EMPTY;
import static org.apache.iceberg.util.ZOrderByteUtils.longToOrderedBytes;

class SparkZOrderUDFUtils  implements Serializable {

  static UDF1<Long, byte[]> longToBytesUDF(ByteBuffer buffer, Object[] candidateBounds) {
    return value -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return longToOrderedBytes(longGetBound(value, candidateBounds), buffer).array();
    };
  }

  static UserDefinedFunction toUDF(UDF1 udf,String udfName){
   return functions.udf(udf, DataTypes.BinaryType).withName(udfName);
  }




  static UDF1<Double, byte[]> doubleToBytesUDF(ByteBuffer buffer, Object[] candidateBounds) {
    return value -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return longToOrderedBytes(longGetBound(Double.doubleToLongBits(value), candidateBounds), buffer).array();
    };
  }

  static UDF1<Float, byte[]> floatToBytesUDF(ByteBuffer buffer, Object[] candidateBounds) {
    return value -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return longToOrderedBytes(getLongBound(Double.doubleToLongBits(value), candidateBounds), buffer).array();
    };
  }

  static UDF1<Integer, byte[]> integerToBytesUDF(ByteBuffer buffer, Object[] candidateBounds) {
    return (Integer value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return longToOrderedBytes(getLongBound(value.longValue(), candidateBounds), buffer).array();
    };
  }

  static UDF1<Short, byte[]> shortToBytesUDF(ByteBuffer buffer, Object[] candidateBounds) {
    return (Short value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return longToOrderedBytes(getLongBound(value.longValue(), candidateBounds), buffer).array();
    };
  }

  static UDF1<String, byte[]> stringToBytesUDF(ByteBuffer buffer, Object[] candidateBounds) {
    return (String value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return longToOrderedBytes(getStringBound(value, candidateBounds), buffer).array();
    };
  }

  static UDF1<Date, byte[]> dateToBytesUDF(ByteBuffer buffer, Object[] candidateBounds) {
    return (Date value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return longToOrderedBytes(getLongBound(value.getTime(), candidateBounds), buffer).array();
    };
  }

  static UDF1<Timestamp, byte[]> timestampToBytesUDF(ByteBuffer buffer, Object[] candidateBounds) {
    return (Timestamp value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return longToOrderedBytes(getLongBound(value.getTime(), candidateBounds), buffer).array();
    };
  }

  static UDF1<Byte, byte[]> byteToBytesUDF(ByteBuffer buffer, Object[] candidateBounds) {
    return value -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return longToOrderedBytes(getLongBound((long) value, candidateBounds), buffer).array();
    };
  }

  static UDF1<BigDecimal, byte[]> decimalToBytesUDF(ByteBuffer buffer, Object[] candidateBounds) {
    return (BigDecimal value) -> {
      if (value == null) {
        return PRIMITIVE_EMPTY;
      }
      return longToOrderedBytes(getLongBound(value.longValue(), candidateBounds), buffer).array();
    };
  }

  /**
   * TODO xzw
   */
  public static int getStringBound(String key, Object[] candidateBounds) {
    return getBound(key, candidateBounds, (o, o2) -> gt((String) o, (String) o2));
  }

  public static int getLongBound(Long key, Object[] candidateBounds) {
    return getBound(key, candidateBounds, (o, o2) -> gt((Long) o, (Long) o2));
  }

  /**
   * 确定单条数据的边界下标索引
   */

  private static int getBound(Object key, Object[] candidateBounds, BiFunc f) {
    int bound = 0;
    if (candidateBounds.length <= 128) {
      while (bound < candidateBounds.length && f.apply(key, candidateBounds[bound])) {
        bound += 1;
      }
    } else {
      bound = Arrays.binarySearch(candidateBounds, key);
      if (bound < 0) {
        bound = -bound - 1;
      }
      if (bound > candidateBounds.length) {
        bound = candidateBounds.length;
      }
    }
    return bound;
  }
  private static int longGetBound(Object key, Object[] candidateBounds) {
    int bound = 0;
    if (candidateBounds.length <= 128) {
      while (bound < candidateBounds.length && gt((Long) key, (Long) candidateBounds[bound])) {
        bound += 1;
      }
    } else {
      bound = Arrays.binarySearch(candidateBounds, key);
      if (bound < 0) {
        bound = -bound - 1;
      }
      if (bound > candidateBounds.length) {
        bound = candidateBounds.length;
      }
    }
    return bound;
  }
  private static int stringGetBound(Object key, Object[] candidateBounds) {
    int bound = 0;
    if (candidateBounds.length <= 128) {
      while (bound < candidateBounds.length && gt((String) key, (String) candidateBounds[bound])) {
        bound += 1;
      }
    } else {
      bound = Arrays.binarySearch(candidateBounds, key);
      if (bound < 0) {
        bound = -bound - 1;
      }
      if (bound > candidateBounds.length) {
        bound = candidateBounds.length;
      }
    }
    return bound;
  }

  private static boolean gt(long x, long y) {
    return x > y;
  }

  private static boolean gt(String x, String y) {
    return x.compareTo(y) > 0;
  }


  interface BiFunc extends BiFunction<Object,Object,Boolean>,Serializable{

  }
}
