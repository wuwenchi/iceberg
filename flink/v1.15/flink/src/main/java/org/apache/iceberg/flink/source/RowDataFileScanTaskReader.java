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
package org.apache.iceberg.flink.source;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

@Internal
public class RowDataFileScanTaskReader extends BaseScanTaskReader<RowData, FileScanTask> {

  private final Schema tableSchema;

  public RowDataFileScanTaskReader(
      Schema tableSchema,
      Schema projectedSchema,
      boolean caseSensitive,
      String nameMapping,
      ScanTaskGroup<FileScanTask> taskGroup,
      FileIO io,
      EncryptionManager encryption) {
    super(projectedSchema, caseSensitive, nameMapping, taskGroup, io, encryption);
    this.tableSchema = tableSchema;
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(FileScanTask task) {
    return Stream.concat(Stream.of(task.file()), task.deletes().stream());
  }

  @Override
  public CloseableIterator<RowData> open(FileScanTask scanTask) {
    if (scanTask.isDataTask()) {
      throw new UnsupportedOperationException("Cannot read data task.");
    }

    Schema partitionSchema =
        TypeUtil.select(projectedSchema(), scanTask.spec().identitySourceIds());

    DataFile file = scanTask.file();
    String filePath = scanTask.file().path().toString();

    FileFormat format = file.format();
    long start = scanTask.start();
    long length = scanTask.length();
    Expression residual = scanTask.residual();
    InputFile inputFile = getInputFile(filePath);

    Map<Integer, ?> idToConstant =
        partitionSchema.columns().isEmpty()
            ? ImmutableMap.of()
            : PartitionUtil.constantsMap(scanTask, RowDataUtil::convertConstant);

    FlinkDeleteFilter deletes =
        new FlinkDeleteFilter(filePath, scanTask.deletes(), tableSchema, projectedSchema());
    CloseableIterable<RowData> iterable =
        deletes.filter(
            newIterable(
                inputFile,
                format,
                start,
                length,
                residual,
                deletes.requiredSchema(),
                idToConstant));

    // Project the RowData to remove the extra meta columns.
    if (!projectedSchema().sameSchema(deletes.requiredSchema())) {
      RowDataProjection rowDataProjection =
          RowDataProjection.create(
              deletes.requiredRowType(),
              deletes.requiredSchema().asStruct(),
              projectedSchema().asStruct());
      iterable = CloseableIterable.transform(iterable, rowDataProjection::wrap);
    }

    return iterable.iterator();
  }
}
