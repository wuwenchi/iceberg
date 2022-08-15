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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.SparkDistributionAndOrderingUtil;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.spark.actions.ZOrderOptions.BUILD_RANGE_SAMPLE_SIZE_KEY;
import static org.apache.iceberg.spark.actions.ZOrderOptions.MAX_OUTPUT_SIZE_KEY;
import static org.apache.iceberg.spark.actions.ZOrderOptions.SAMPLE_POINTS_PER_PARTITION_HINT_KEY;
import static org.apache.iceberg.spark.actions.ZOrderOptions.SPATIAL_CURVE_STRATEGY_TYPE_KEY;
import static org.apache.iceberg.spark.actions.ZOrderOptions.VAR_LENGTH_CONTRIBUTION_KEY;

public class SparkZOrderStrategy extends SparkSortStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(SparkZOrderStrategy.class);
  private static final String Z_COLUMN = "ICEZVALUE";
  private static final Schema Z_SCHEMA = new Schema(Types.NestedField.required(0, Z_COLUMN, Types.BinaryType.get()));
  private static final org.apache.iceberg.SortOrder Z_SORT_ORDER = org.apache.iceberg.SortOrder.builderFor(Z_SCHEMA)
      .sortBy(Z_COLUMN, SortDirection.ASC, NullOrder.NULLS_LAST)
      .build();

  private final List<String> zOrderColNames;
  ZOrderUDFBuilder zOrderUDFBuilder;

  public SparkZOrderStrategy(Table table, SparkSession spark, List<String> zOrderColNames) {
    super(table, spark);

    Preconditions.checkArgument(
        zOrderColNames != null && !zOrderColNames.isEmpty(),
        "Cannot ZOrder when no columns are specified");

    Stream<String> identityPartitionColumns =
        table.spec().fields().stream().filter(f -> f.transform().isIdentity()).map(PartitionField::name);
    List<String> partZOrderCols =
        identityPartitionColumns.filter(zOrderColNames::contains).collect(Collectors.toList());

    if (!partZOrderCols.isEmpty()) {
      LOG.warn("Cannot ZOrder on an Identity partition column as these values are constant within a partition "
          + "and will be removed from the ZOrder expression: {}", partZOrderCols);
      zOrderColNames.removeAll(partZOrderCols);
      Preconditions.checkArgument(
          !zOrderColNames.isEmpty(),
          "Cannot perform ZOrdering, all columns provided were identity "
              + "partition columns and cannot be used.");
    }

    this.zOrderColNames = zOrderColNames;
    zOrderUDFBuilder = new ZOrderUDFBuilder(spark());
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(VAR_LENGTH_CONTRIBUTION_KEY)
        .add(MAX_OUTPUT_SIZE_KEY)
        .add(SPATIAL_CURVE_STRATEGY_TYPE_KEY)
        .add(BUILD_RANGE_SAMPLE_SIZE_KEY)
        .add(SAMPLE_POINTS_PER_PARTITION_HINT_KEY)
        .build();
  }

  @Override
  public RewriteStrategy options(Map<String, String> options) {
    super.options(options);
    zOrderUDFBuilder.setOptions(options);
    return this;
  }

  @Override
  public String name() {
    return "Z-ORDER";
  }

  @Override
  protected void validateOptions() {
    // Ignore SortStrategy validation
    return;
  }

  @Override
  public Set<DataFile> rewriteFiles(List<FileScanTask> filesToRewrite) {

    String groupID = UUID.randomUUID().toString();
    boolean requiresRepartition = !filesToRewrite.get(0).spec().equals(table().spec());

    SortOrder[] ordering;
    if (requiresRepartition) {
      ordering = SparkDistributionAndOrderingUtil.convert(SortOrderUtil.buildSortOrder(table(), sortOrder()));
    } else {
      ordering = SparkDistributionAndOrderingUtil.convert(sortOrder());
    }

    Distribution distribution = Distributions.ordered(ordering);

    try {
      manager().stageTasks(table(), groupID, filesToRewrite);

      // Disable Adaptive Query Execution as this may change the output partitioning of our write
      SparkSession cloneSession = spark().cloneSession();
      cloneSession.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);

      // Reset Shuffle Partitions for our sort
      long numOutputFiles = numOutputFiles((long) (inputFileSize(filesToRewrite) * sizeEstimateMultiple()));
      cloneSession.conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), Math.max(1, numOutputFiles));

      Dataset<Row> scanDF = cloneSession.read().format("iceberg")
          .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID).load(table().name());

      Column[] originalColumns = Arrays.stream(scanDF.schema().names()).map(functions::col).toArray(Column[]::new);
      List<StructField> zOrderColumns =
          zOrderColNames.stream().map(scanDF.schema()::apply).collect(Collectors.toList());

      BaseSparkZOrderUDF zOrderUDF =
          zOrderUDFBuilder.setZOrderColumns(zOrderColumns).setScanDF(scanDF).setNumCols(zOrderColNames.size()).build();

      Dataset<Row> zvalueDF = scanDF.withColumn(Z_COLUMN, zOrderUDF.interleaveBytes(zOrderUDF.compute()));

      SQLConf sqlConf = cloneSession.sessionState().conf();
      LogicalPlan sortPlan = sortPlan(distribution, ordering, zvalueDF.logicalPlan(), sqlConf);
      Dataset<Row> sortedDf = new Dataset<>(cloneSession, sortPlan, zvalueDF.encoder());
      sortedDf.select(originalColumns).write().format("iceberg")
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupID)
          .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
          .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
          .mode("append")
          .save(table().name());

      return rewriteCoordinator().fetchNewDataFiles(table(), groupID);
    } finally {
      manager().removeTasks(table(), groupID);
      rewriteCoordinator().clearRewrite(table(), groupID);
    }
  }

  @Override
  protected org.apache.iceberg.SortOrder sortOrder() {
    return Z_SORT_ORDER;
  }
}
