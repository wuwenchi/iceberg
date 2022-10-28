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

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.FlinkAvroReader;
import org.apache.iceberg.flink.data.FlinkOrcReader;
import org.apache.iceberg.flink.data.FlinkParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;

@Internal
public abstract class BaseScanTaskReader<T, S extends ScanTask>
    implements ScanTaskReader<T, S>, Serializable, CloseableIterator<T> {

  private final Schema projectedSchema;
  private final boolean caseSensitive;
  private final NameMapping nameMapping;
  private ScanTaskGroup<S> taskGroup;
  private Map<String, InputFile> lazyInputFiles;
  private final FileIO io;
  private final EncryptionManager encryption;

  private Iterator<S> tasks;

  private CloseableIterator<T> currentIterator;
  private int fileOffset;
  private long recordOffset;

  public BaseScanTaskReader(
      Schema projectedSchema,
      boolean caseSensitive,
      String nameMapping,
      ScanTaskGroup<S> taskGroup,
      FileIO io,
      EncryptionManager encryption) {
    this.io = io;
    this.encryption = encryption;
    this.projectedSchema = projectedSchema;
    this.caseSensitive = caseSensitive;
    this.nameMapping = nameMapping != null ? NameMappingParser.fromJson(nameMapping) : null;
    this.taskGroup = taskGroup;

    this.tasks = taskGroup.tasks().iterator();
    this.currentIterator = CloseableIterator.empty();

    // fileOffset starts at -1 because we started
    // from an empty iterator that is not from the split files.
    this.fileOffset = -1;
    // record offset points to the record that next() should return when called
    this.recordOffset = 0L;
  }

  @Override
  public void taskGroup(ScanTaskGroup<S> newTaskGroup) {
    this.taskGroup = newTaskGroup;
    this.lazyInputFiles = null;
  }

  @Override
  public ScanTaskGroup<S> taskGroup() {
    return taskGroup;
  }

  public Schema projectedSchema() {
    return projectedSchema;
  }

  protected abstract Stream<ContentFile<?>> referencedFiles(S task);

  /**
   * (startingFileOffset, startingRecordOffset) points to the next row that reader should resume
   * from. E.g., if the seek position is (file=0, record=1), seek moves the iterator position to the
   * 2nd row in file 0. When next() is called after seek, 2nd row from file 0 should be returned.
   */
  public void seek(int startingFileOffset, long startingRecordOffset) {
    Preconditions.checkState(
        fileOffset == -1, "Seek should be called before any other iterator actions");
    // skip files
    Preconditions.checkState(
        startingFileOffset < taskGroup.tasks().size(),
        "Invalid starting file offset %s for combined scan task with %s files: %s",
        startingFileOffset,
        taskGroup.tasks().size(),
        taskGroup);
    for (long i = 0L; i < startingFileOffset; ++i) {
      tasks.next();
    }

    updateCurrentIterator();
    // skip records within the file
    for (long i = 0; i < startingRecordOffset; ++i) {
      if (currentFileHasNext() && hasNext()) {
        next();
      } else {
        throw new IllegalStateException(
            String.format(
                "Invalid starting record offset %d for file %d from CombinedScanTask: %s",
                startingRecordOffset, startingFileOffset, taskGroup));
      }
    }

    fileOffset = startingFileOffset;
    recordOffset = startingRecordOffset;
  }

  @Override
  public boolean hasNext() {
    updateCurrentIterator();
    return currentIterator.hasNext();
  }

  @Override
  public T next() {
    updateCurrentIterator();
    recordOffset += 1;
    return currentIterator.next();
  }

  public boolean currentFileHasNext() {
    return currentIterator.hasNext();
  }

  /** Updates the current iterator field to ensure that the current Iterator is not exhausted. */
  private void updateCurrentIterator() {
    try {
      while (!currentIterator.hasNext() && tasks.hasNext()) {
        currentIterator.close();
        currentIterator = openTaskIterator(tasks.next());
        fileOffset += 1;
        recordOffset = 0L;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private CloseableIterator<T> openTaskIterator(S scanTask) {
    return open(scanTask);
  }

  @Override
  public void close() throws IOException {
    // close the current iterator
    currentIterator.close();
    taskGroup = null;
    tasks = null;
  }

  public int fileOffset() {
    return fileOffset;
  }

  public long recordOffset() {
    return recordOffset;
  }

  protected InputFile getInputFile(String location) {
    return inputFiles().get(location);
  }

  private Map<String, InputFile> inputFiles() {
    if (lazyInputFiles == null) {
      Stream<EncryptedInputFile> encryptedFiles =
          taskGroup.tasks().stream().flatMap(this::referencedFiles).map(this::toEncryptedInputFile);

      // decrypt with the batch call to avoid multiple RPCs to a key server, if possible
      Iterable<InputFile> decryptedFiles = encryption.decrypt(encryptedFiles::iterator);

      Map<String, InputFile> files = Maps.newHashMap();
      decryptedFiles.forEach(decrypted -> files.putIfAbsent(decrypted.location(), decrypted));
      this.lazyInputFiles = ImmutableMap.copyOf(files);
    }

    return lazyInputFiles;
  }

  private EncryptedInputFile toEncryptedInputFile(ContentFile<?> file) {
    InputFile inputFile = io.newInputFile(file.path().toString());
    return EncryptedFiles.encryptedInput(inputFile, file.keyMetadata());
  }

  protected class FlinkDeleteFilter extends DeleteFilter<RowData> {
    private final RowType requiredRowType;
    private final RowDataWrapper asStructLike;

    FlinkDeleteFilter(
        String filePath, List<DeleteFile> deletes, Schema tableSchema, Schema requestedSchema) {

      super(filePath, deletes, tableSchema, requestedSchema);
      this.requiredRowType = FlinkSchemaUtil.convert(requiredSchema());
      this.asStructLike = new RowDataWrapper(requiredRowType, requiredSchema().asStruct());
    }

    public RowType requiredRowType() {
      return requiredRowType;
    }

    @Override
    protected StructLike asStructLike(RowData row) {
      return asStructLike.wrap(row);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return BaseScanTaskReader.this.getInputFile(location);
    }
  }

  protected CloseableIterable<RowData> newIterable(
      InputFile file,
      FileFormat format,
      long start,
      long length,
      Expression residual,
      Schema schema,
      Map<Integer, ?> idToConstant) {
    CloseableIterable<RowData> iter;

    switch (format) {
      case PARQUET:
        iter = newParquetIterable(file, start, length, residual, schema, idToConstant);
        break;

      case AVRO:
        iter = newAvroIterable(file, start, length, schema, idToConstant);
        break;

      case ORC:
        iter = newOrcIterable(file, start, length, residual, schema, idToConstant);
        break;

      default:
        throw new UnsupportedOperationException("Cannot read unknown format: " + format);
    }

    return iter;
  }

  protected CloseableIterable<RowData> newAvroIterable(
      InputFile file, long start, long length, Schema schema, Map<Integer, ?> idToConstant) {
    Avro.ReadBuilder builder =
        Avro.read(file)
            .reuseContainers()
            .project(schema)
            .split(start, length)
            .createReaderFunc(readSchema -> new FlinkAvroReader(schema, readSchema, idToConstant));

    if (nameMapping != null) {
      builder.withNameMapping(nameMapping);
    }

    return builder.build();
  }

  protected CloseableIterable<RowData> newParquetIterable(
      InputFile file,
      long start,
      long length,
      Expression residual,
      Schema schema,
      Map<Integer, ?> idToConstant) {
    Parquet.ReadBuilder builder =
        Parquet.read(file)
            .split(start, length)
            .project(schema)
            .createReaderFunc(
                fileSchema -> FlinkParquetReaders.buildReader(schema, fileSchema, idToConstant))
            .filter(residual)
            .caseSensitive(caseSensitive)
            .reuseContainers();

    if (nameMapping != null) {
      builder.withNameMapping(nameMapping);
    }

    return builder.build();
  }

  protected CloseableIterable<RowData> newOrcIterable(
      InputFile file,
      long start,
      long length,
      Expression residual,
      Schema schema,
      Map<Integer, ?> idToConstant) {
    Schema readSchemaWithoutConstantAndMetadataFields =
        TypeUtil.selectNot(
            schema, Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

    ORC.ReadBuilder builder =
        ORC.read(file)
            .project(readSchemaWithoutConstantAndMetadataFields)
            .split(start, length)
            .createReaderFunc(
                readOrcSchema -> new FlinkOrcReader(schema, readOrcSchema, idToConstant))
            .filter(residual)
            .caseSensitive(caseSensitive);

    if (nameMapping != null) {
      builder.withNameMapping(nameMapping);
    }

    return builder.build();
  }
}
