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
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.apache.iceberg.util.ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;

abstract class BaseSparkZOrderUDF implements Serializable {
  static final byte[] PRIMITIVE_EMPTY = new byte[PRIMITIVE_BUFFER_SIZE];
  protected final int numCols;
  protected final Map<String, SparkZOrderUDFProvider> udfMap = Maps.newHashMap();
  private final int maxOutputSize;
  protected Set<String> supportedTypes;
  protected int inputCol = 0;
  protected int totalOutputBytes = 0;
  protected transient ThreadLocal<CharsetEncoder> encoder;
  /**
   * Every Spark task runs iteratively on a rows in a single thread so ThreadLocal should protect from
   * concurrent access to any of these structures.
   */
  private transient ThreadLocal<ByteBuffer[]> inputBuffers;
  private transient ThreadLocal<ByteBuffer> outputBuffer;
  private transient ThreadLocal<byte[][]> inputHolder;
  private final UserDefinedFunction interleaveUDF = functions
      .udf((Seq<byte[]> arrayBinary) -> interleaveBits(arrayBinary), DataTypes.BinaryType)
      .withName("INTERLEAVE_BYTES");

  BaseSparkZOrderUDF(int numCols, int maxOutputSize) {
    this.numCols = numCols;
    this.maxOutputSize = maxOutputSize;
    init();
  }

  protected boolean supportType(DataType type) {
    return supportedTypes.contains(type.typeName());
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    inputBuffers = ThreadLocal.withInitial(() -> new ByteBuffer[numCols]);
    inputHolder = ThreadLocal.withInitial(() -> new byte[numCols][]);
    outputBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(totalOutputBytes));
    encoder = ThreadLocal.withInitial(StandardCharsets.UTF_8::newEncoder);
  }

  protected ByteBuffer inputBuffer(int position, int size) {
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

  private void increaseOutputSize(int bytes) {
    totalOutputBytes = Math.min(totalOutputBytes + bytes, maxOutputSize);
  }

  protected void increase(int primitiveBufferSize) {
    this.inputCol++;
    increaseOutputSize(primitiveBufferSize);
  }

  Column interleaveBytes(Column arrayBinary) {
    return interleaveUDF.apply(arrayBinary);
  }

  abstract Column applyToBytesUdf(Column column, DataType type, Object obj);
  abstract void init();

  Column sorted(Column column, DataType type, Object o) {
    return applyToBytesUdf(column, type, o);
  }

  interface SparkZOrderUDFProvider extends Serializable {

    UserDefinedFunction getUDF(Object obj);
  }
}
