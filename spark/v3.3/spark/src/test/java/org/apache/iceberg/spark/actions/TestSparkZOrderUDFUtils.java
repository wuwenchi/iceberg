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

import org.junit.Assert;
import org.junit.Test;

public class TestSparkZOrderUDFUtils {

  @Test
  public void testBound(){
    // Make sure the boundary subscripts are as expected
    Assert.assertEquals(
        "Index subscript must be 1",
        1, SparkZOrderUDFUtils.getLongBound(20L, new long[] {10L, 20L, 30L}));
    Assert.assertEquals(
        "Index subscript must be 1",
        1, SparkZOrderUDFUtils.getStringBound("f", new String[] {"c", "f", "i"}));
    Assert.assertEquals(
        "Index subscript must be 1",
        1, SparkZOrderUDFUtils.getStringBound("F", new String[] {"C", "F", "I"}));
  }
}
