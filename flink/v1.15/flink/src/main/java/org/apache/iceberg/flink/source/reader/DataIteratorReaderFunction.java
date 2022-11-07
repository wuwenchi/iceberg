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
package org.apache.iceberg.flink.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.flink.source.BaseScanTaskReader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;

/** A {@link ReaderFunction} implementation that uses {@link BaseScanTaskReader}. */
public abstract class DataIteratorReaderFunction<T, S extends ScanTask, G extends ScanTaskGroup<S>>
    implements ReaderFunction<T, S, G> {
  private final DataIteratorBatcher<T, S> batcher;

  public DataIteratorReaderFunction(DataIteratorBatcher<T, S> batcher) {
    this.batcher = batcher;
  }

  protected abstract BaseScanTaskReader<T, S> createDataIterator(IcebergSourceSplit<S, G> split);

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> apply(
      IcebergSourceSplit<S, G> split) {
    BaseScanTaskReader<T, S> inputIterator = createDataIterator(split);
    inputIterator.seek(split.fileOffset(), split.recordOffset());
    return batcher.batch(split.splitId(), inputIterator);
  }
}
