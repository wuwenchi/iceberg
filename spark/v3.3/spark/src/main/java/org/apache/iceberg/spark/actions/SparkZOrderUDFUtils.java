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
import java.util.Arrays;

class SparkZOrderUDFUtils implements Serializable {

  /**
   * Compute the bound subscripts
   */
  static int getLongBound(long key, long[] candidateBounds) {
    int bound = 0;
    if (candidateBounds.length <= 128) {
      while (bound < candidateBounds.length && key > candidateBounds[bound]) {
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

  static int getStringBound(String key, String[] candidateBounds) {
    int bound = 0;
    if (candidateBounds.length <= 128) {
      while (bound < candidateBounds.length && key.compareTo(candidateBounds[bound]) > 0) {
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
}
