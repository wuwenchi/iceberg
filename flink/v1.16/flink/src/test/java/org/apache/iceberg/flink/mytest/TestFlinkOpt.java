/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

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

package org.apache.iceberg.flink.mytest;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.bucket;
// import static org.apache.iceberg.flink.source.ScanMode.CHANGELOG_SCAN;

public class TestFlinkOpt {
  static String dbName;
  static String tableName;
  static String tableLocation;
  static Catalog catalog;
  static HiveCatalog hiveCatalog;
  static String catalogName;
  static String warehouse;
  static TableLoader tableLoader;
  static CatalogLoader catalogLoader;
  static TableIdentifier tableId;
  static String hdfs;
  static String uri;
  static int inputCnt;
  static int inputStep;
  static int inputOffset;
  static boolean drop;
  static int parallelism;
  static int method;
  static String localPath;
  static HashMap<String, String> scanConf = new HashMap<>();

  static long begin;
  static StreamExecutionEnvironment env;
  static StreamTableEnvironment tEnv;

  @Before
  public void env() {
    Logger.getRootLogger().setLevel(Level.ERROR);
    System.setProperty("HADOOP_USER_NAME", "root");
    //        System.setProperty("user.name", "abcde");
    parameters();
    tableId = TableIdentifier.of(dbName, tableName);

    if (method == 1 || method == 2) {
      if (method == 1) {
        warehouse = localPath;
      } else {
        warehouse = hdfs;
      }
      tableLocation = warehouse + "/" + dbName + "/" + tableName;
      createHadoopCatalog();
    } else if (method == 3) {
      createHiveCatalog();
    }
    getEnv();
    begin = System.currentTimeMillis();
  }

  @After
  public void end() {
    System.out.println("total : " + (System.currentTimeMillis() - begin) / 1000 + "s");
  }

  public void createHadoopCatalog() {
    Configuration conf = new Configuration();
    catalog = new HadoopCatalog(conf, warehouse);
    tableLoader = TableLoader.fromHadoopTable(tableLocation, new Configuration());
  }

  public void createHiveCatalog() {
    // 配置 hive 的 catalog 属性
    Map<String, String> properties = new HashMap<>();
    properties.put("warehouse", hdfs);
    properties.put("uri", uri);

    hiveCatalog = new HiveCatalog();
    Configuration config = new Configuration();
    config.set("metastore.catalog.default", catalogName);
    hiveCatalog.setConf(config);
    hiveCatalog.initialize(catalogName, properties);  // 这里使用的 catalogName 没什么用，主要是上面的 catalog 的 default 配置
    catalog = hiveCatalog;
    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    hiveCatalog.listNamespaces().forEach(System.out::println);
    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

    // 设置 tableLoader
    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    conf.set("hive.metastore.uris", uri);
    conf.set("hive.metastore.warehouse.dir", hdfs);
    conf.set("hive.metastore.schema.verification", "false");
    conf.set("datanucleus.schema.autoCreateTables", "true");

    catalogLoader = CatalogLoader.hive(catalogName, conf, new HashMap<>());
    tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);
  }

  public void parameters() {
    parallelism = 1;
    //    hdfs = "hdfs://10.0.30.12:9009/user/wuwenchi/warehouse";
    hdfs = "hdfs://10.0.30.12:9009/user/hive/warehouse";
    uri = "thrift://10.0.30.12:9083";
    //        uri = "thrift://10.201.0.212:34586";
    catalogName = "hive";
    //        catalogName = "iceberg_default_374";
    //    catalogName = "custom_catalog2";
    dbName = "abc";
    tableName = "tb1";
    //        dbName = "aaa";
    //        tableName = "account_orc222";
    //        tableName = "cx_01";
    inputCnt = 1;
    inputStep = 1;
    inputOffset = 0;
    drop = true;
    localPath = "file:///Users/wuwenchi/github/iceberg/warehouse";
    method = 1;

    System.out.println("===================================================");
    //        System.out.println(System.getProperty("user.dir"));
    System.out.println("parallelism:  " + parallelism);
    System.out.println("uri:          " + uri);
    System.out.println("hdfs:         " + hdfs);
    System.out.println("catalog:      " + catalogName);
    System.out.println("database:     " + dbName);
    System.out.println("table:        " + tableName);
    System.out.println("cnt:          " + inputCnt);
    System.out.println("rows:         " + inputStep);
    System.out.println("drop:         " + drop);
    System.out.println("localPath:    " + localPath);
    System.out.println("method:       " + method);
    System.out.println("===================================================");
  }

  @Test
  public void testFlinkOpt() throws Exception {
    create();
    write();
    //        read();
    //    sqlCreate();
    //    sqlRead();
    //    rewriteTable();
    //    createHiveDataWithSql();
    //    rewriteTable();
  }

  @Test
  public void create() {
    if (catalog.tableExists(tableId)) {
      if (drop) {
        catalog.dropTable(tableId);
      } else {
        return;
      }
    }

    //创建表
    List<Types.NestedField> columns = new ArrayList<>();
    //    columns.add(Types.NestedField.required(11, "c1", Types.IntegerType.get()));
    //    columns.add(Types.NestedField.required(12, "c2", Types.IntegerType.get()));
    //    columns.add(Types.NestedField.required(13, "c3", Types.IntegerType.get()));
    //    columns.add(Types.NestedField.required(14, "c4", Types.IntegerType.get()));
    //    columns.add(Types.NestedField.required(15, "c5", Types.IntegerType.get()));
    //    columns.add(Types.NestedField.required(16, "c6", Types.IntegerType.get()));

    columns.add(Types.NestedField.required(11, "id1", Types.IntegerType.get()));
    columns.add(Types.NestedField.required(12, "id2", Types.IntegerType.get()));
    columns.add(Types.NestedField.required(13, "par", Types.StringType.get()));
    columns.add(Types.NestedField.optional(14, "c4", Types.StringType.get()));
    columns.add(Types.NestedField.optional(15, "c5", Types.BinaryType.get()));

    HashSet<Integer> identifierFieldIds = new HashSet<>();
    identifierFieldIds.add(11);
    identifierFieldIds.add(12);
    identifierFieldIds.add(13);
    Schema schema = new Schema(columns, identifierFieldIds);
    //    Schema schema = new Schema(columns);

    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
        .identity("par")
        //            .bucket("c2", 4)
        //            .day("c2")
        // .truncate("c4", 4)
        // .truncate("c2", 4)
        .build();

    Map<String, String> tableProperties = Maps.newHashMap();
    //        tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true"); // engine.hive.enabled=true
    //    tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "orc");
    //    tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
    tableProperties.put(TableProperties.FORMAT_VERSION, "2");//v2 版本才支持delete file
    //    tableProperties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(1024));
    tableProperties.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(512 * 1024 * 1024));
    //    tableProperties.put(TableProperties.PARQUET_PAGE_SIZE_BYTES, String.valueOf(128*1024));
    tableProperties.put(TableProperties.UPSERT_ENABLED, "true");

    Table table = catalog.createTable(tableId, schema, partitionSpec, tableProperties);
    System.out.println(table.location());
  }

  @Test
  public void write() throws Exception {
    //    StreamExecutionEnvironment env = getEnv();
    env.setParallelism(parallelism);

    for (int i = 0; i < inputCnt; i++) {
      long tmp = System.currentTimeMillis();
      //      DataStream<RowData> input = env.addSource(new IntSrcFunc(i, 10));
      //      DataStream<RowData> input = env.addSource(new IntParaSrcFunc(i, 10));
      DataStream<RowData> input = env.addSource(new EqualDelete(i, inputStep, inputOffset));

      DataStreamSink<Void> append = FlinkSink.forRowData(input)
          .tableLoader(tableLoader)
          //              .table(table)
          //              .tableLoader(sourceTable)
          .overwrite(false)
          .distributionMode(DistributionMode.HASH)
          //              .equalityFieldColumns(Lists.newArrayList("c1"))
          .append();

      System.out.println(input.getParallelism());
      env.execute();
      System.out.println("spent : " + i + " time : " + (System.currentTimeMillis() - tmp) / 1000);
    }
  }

  @Test
  public void updatePartition() {
    tableLoader.open();
    ;
    UpdatePartitionSpec updatePartitionSpec = tableLoader.loadTable().updateSpec();
    updatePartitionSpec.addField(bucket("c1", 4)).commit();
  }

  @Test
  public void createDatabase() {
    hiveCatalog.createNamespace(Namespace.of(dbName));
  }

  @Test
  public void read() throws Exception {
    DataStream<RowData> dataStream = FlinkSource.forRowData()
        //            .env(getEnv())
        //            .startSnapshotId()
        .env(env)
        .tableLoader(tableLoader)
        .streaming(false)
        //            .snapshotId()
        .build();
    // TODO 如果数据量特别大的话，collect 会出现爆内存吗？
    CloseableIterator<RowData> dataCloseableIterator = dataStream.executeAndCollect();
    int i = 0;
    System.out.println("------------------------------------------------------------");
    //    while (i < 50 && dataCloseableIterator.hasNext()) {
    while (dataCloseableIterator.hasNext()) {
      RowData d = dataCloseableIterator.next();
      //      System.out.println(d.getInt(0) + "," + d.getString(1) + "," + d.getString(2));
      //      System.out.println(d.getInt(0) + "," + d.getString(1));
      //            System.out.println(d.getInt(0) + "," + d.getTimestamp(1,6));
      System.out.println(d.getInt(0));
      i++;
    }
    System.out.println("total: " + i);
    System.out.println("------------------------------------------------------------");
  }

  @Test
  public void setScanConf() {
    scanConf.put("streaming", "true");
    // scanConf.put("scan-mode", CHANGELOG_SCAN.toString());
  }

  @Test
  public void readChangeLog() {
    // 在 ScanContext 里会读取这些配置
    scanConf.put("streaming", "false");
    // scanConf.put("scan-mode", CHANGELOG_SCAN.toString());
    ArrayList<String> conf = new ArrayList<>();
    scanConf.forEach((key, value) -> {
      String stringBuffer = '\'' +
          key +
          '\'' +
          '=' +
          '\'' +
          value +
          '\'';
      conf.add(stringBuffer);
    });
    String join = String.join(",", conf);
    System.out.println(join);
    tEnv.sqlQuery("SELECT * FROM " + tableName + " /*+ OPTIONS (" + join + " )*/ ")
        .execute().print();
  }

  @Test
  public void dbs() {
    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    hiveCatalog.listNamespaces().forEach(System.out::println);
    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    catalog.listTables(Namespace.of(dbName)).forEach(System.out::println);
    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    System.out.println(catalogName + "." + dbName + "." + tableName + "  exist? " + catalog.tableExists(tableId));
    //    System.out.println(envTbl.sqlQuery("show databases;"));
    //    envTbl.executeSql("show databases").print();
  }

  @Test
  public void testMetaTable() {
    //        MetadataTableUtils.createMetadataTableInstance();
  }

  @Test
  public void sqlCreate() {
    if (drop) {
      tEnv.executeSql("drop table if exists " + tableName);
    }

    tEnv.executeSql("create table if not exists " + tableName + " (\n" +
        "id int, \n" +
        "id2 as id * 2, \n" +
        "f1 as TO_TIMESTAMP(FROM_UNIXTIME(id*3)), \n" +
        "t1 timestamp(6), \n" +
        "t2 as cast(t1 as timestamp(2)), \n" +
        "watermark for t2 as t2 - INTERVAL '5' SECOND ,\n" +
        "primary key (id) not enforced ) \n" +
        "with (" +
        "'write.format.default'='parquet'," +
        "'format-version'='2'" +
        ")");
    tEnv.executeSql("desc " + tableName).print();

    tEnv.executeSql("insert into " + tableName + " values (1, TO_TIMESTAMP(FROM_UNIXTIME(24)))").print();
    tEnv.sqlQuery("select * from " + tableName).execute().print();

    tEnv.executeSql("use catalog default_catalog");
    tEnv.executeSql("use default_database");
    System.out.println("===============================================================================");
    Arrays.stream(tEnv.listDatabases()).collect(Collectors.toList()).forEach(System.out::println);
    tEnv.executeSql("create table iceberg_table (\n" +
        " id    int,\n" +
        "t1 timestamp(6), \n" +
        "t3 as cast(t1 as timestamp(2)), \n" +
        "watermark for t3 as t3 - INTERVAL '5' SECOND \n" +
        //            " WATERMARK FOR order_time AS order_time - INTERVAL '5' MINUTE\n" +
        ") WITH (\n" +
        " 'connector' = 'iceberg',\n" +
        "'catalog-type' = 'hadoop', \n" +
        " 'catalog-name' = 'hadoop_catalog',\n" +
        " 'catalog-database' = 'abc',\n" +
        " 'catalog-table' = 'tb1',\n" +
        "  'warehouse'='" + warehouse + "'" +
        ")"
    );
    tEnv.sqlQuery("select * from iceberg_table").execute().print();
  }

  @Test
  public void sqlRead() {
    tEnv.executeSql("desc " + tableName).print();
    tEnv.executeSql("insert into " + tableName + " values (1, 'a', TO_TIMESTAMP(FROM_UNIXTIME(24)))").print();
    tEnv.sqlQuery("select * from " + tableName).execute().print();
    catalog.loadTable(tableId).properties().forEach((k, v) -> {
      if (k.startsWith("flink")) {
        System.out.println(k + ", " + v);
      }
    });
  }

  static public void getEnv() {
    org.apache.flink.configuration.Configuration confData = new org.apache.flink.configuration.Configuration();
    confData.setString("akka.ask.timeout", "1h");
    confData.setString("akka.watch.heartbeat.interval", "1h");
    confData.setString("akka.watch.heartbeat.pause", "1h");
    confData.setString("heartbeat.timeout", "18000000");
    confData.setString(String.valueOf(CoreOptions.CHECK_LEAKED_CLASSLOADER), "false");
    confData.setString("rest.bind-port", "8080-9000");
    confData.setString(TableConfigOptions.LOCAL_TIME_ZONE, "UTC");
    confData.setBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
    // confData.setBoolean(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE.key(), true);
    env = StreamExecutionEnvironment.getExecutionEnvironment(confData);
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().withConfiguration(confData).build());
    if (method == 1) {
      tEnv.executeSql("" +
          "CREATE CATALOG hadoop_catalog WITH (\n" +
          "    'type'='iceberg',\n" +
          "    'catalog-type'='hadoop',\n" +
          "    'warehouse'='" + localPath + "'\n" +
          "  )");
      tEnv.executeSql("use catalog hadoop_catalog");
    } else if (method == 3) {
      tEnv.executeSql("" +
          "CREATE CATALOG hive_catalog WITH (\n" +
          "    'type'='iceberg',\n" +
          "    'catalog-type'='hive',\n" +
          "    'uri'='" + uri + "',\n" +
          "    'clients'='5',\n" +
          "    'property-version'='1',\n" +
          "    'warehouse'='" + hdfs + "'\n" +
          "  )");
      tEnv.executeSql("use catalog hive_catalog");
    }
    tEnv.executeSql("use " + dbName);
    tEnv.executeSql("show tables").print();
  }

  @Test
  public void rewriteTable() {
    TableLoader sourceTable = TableLoader.fromHadoopTable(tableLocation, new Configuration());
    sourceTable.open();
    Table table = sourceTable.loadTable();
    table.refresh();
    System.out.println(table.currentSnapshot());
    //    StreamExecutionEnvironment env = getEnv();
    //    env.setParallelism(1);

    RewriteDataFilesActionResult rewriteDataFilesActionResult = Actions.forTable(env, table)
        .rewriteDataFiles()
        .targetSizeInBytes(2200)
        //            .targetSizeInBytes(5050)
        .splitOpenFileCost(1)
        .execute();
    System.out.println(rewriteDataFilesActionResult.deletedDataFiles().size());
    for (DataFile dataFile : rewriteDataFilesActionResult.deletedDataFiles()) {
      System.out.println(dataFile.path().toString());
    }
    System.out.println(rewriteDataFilesActionResult.addedDataFiles().size());
    for (DataFile dataFile : rewriteDataFilesActionResult.addedDataFiles()) {
      System.out.println(dataFile.path().toString());
    }
  }

  @Test
  public void createHiveDataWithSql() {
    EnvironmentSettings settings = EnvironmentSettings
        .newInstance()
        .inBatchMode()
        //            .inStreamingMode()
        .build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    org.apache.flink.configuration.Configuration configuration = tableEnv
        .getConfig()
        .getConfiguration();
    configuration.setString("table.dynamic-table-options.enabled", "true");
    //    configuration.setString("execution.checkpointing.internal", "20sec");

    TableResult tableResult = tableEnv.executeSql("CREATE CATALOG hive_catalog WITH (\n" +
        "  'type'='iceberg',\n" +
        "  'catalog-type'='hive',\n" +
        "  'uri'='" + uri + "', \n" +
        "  'warehouse'='" + hdfs + "',\n" +
        "  'property-version'='1'\n" +
        ")");
    tableResult.print();
    tableEnv.executeSql("USE CATALOG hive_catalog");
    tableEnv.executeSql("SHOW databases").print();
    tableEnv.useDatabase(dbName);
    tableEnv.sqlQuery("select * from " + dbName + "." + tableName).execute().print();
  }

  public static class EqualDelete implements ParallelSourceFunction<RowData> {

    Integer count;
    Integer step;
    Integer offset;

    public EqualDelete(Integer count, Integer step) {
      this.count = count;
      this.step = step;
      this.offset = 0;
    }

    public EqualDelete(Integer count, Integer step, Integer offset) {
      this.count = count;
      this.step = step;
      this.offset = offset;
    }

    @Override
    public void run(SourceContext<RowData> ctx) {
      for (int i = 0; i < step; i++) {
        GenericRowData rowData = new GenericRowData(RowKind.INSERT, 5);
        rowData.setField(0, count * step + i + offset);
        rowData.setField(1, count * step + i + offset);
        //                rowData.setField(0, 1);
        //        rowData.setField(0, (int)(Math.random() * 100000));
        rowData.setField(2, StringData.fromString("a"));
        //        rowData.setField(1, StringData.fromString("abc"));
        rowData.setField(3, StringData.fromString(RandomStringUtils.randomAlphanumeric(5) + "_insert"));
        //        rowData.setField(2, StringData.fromString("abc"));
        byte[] bytes = new byte[] {1, 2, 3, 4, 5, 6};
        rowData.setField(4, bytes);
        ctx.collect(rowData);

        //        GenericRowData rowData3 = new GenericRowData(RowKind.UPDATE_BEFORE, 3);
        ////        rowData3.setField(0, count * step + i + offset);
        //        rowData3.setField(0, 1);
        //        rowData3.setField(1, StringData.fromString("abc"));
        //        rowData3.setField(2, StringData.fromString(RandomStringUtils.randomAlphanumeric(5) + "_before"));
        ////        rowData3.setField(2, StringData.fromString("abc"));
        //        ctx.collect(rowData3);
        ////
        //        GenericRowData rowData2 = new GenericRowData(RowKind.UPDATE_AFTER, 3);
        //        rowData2.setField(0, count * step + i + offset);
        //        rowData2.setField(0, 1);
        //        rowData2.setField(1, StringData.fromString("def"));
        //        rowData2.setField(2, StringData.fromString(RandomStringUtils.randomAlphanumeric(5) + "_after"));
        ////        rowData2.setField(2, StringData.fromString("abc"));
        //        ctx.collect(rowData2);
      }
    }

    @Override
    public void cancel() {
    }
  }

  @Test
  public void createPartitionKeyTable() {
    tEnv.executeSql("drop table if exists " + tableName);
    tEnv.executeSql("create table if not exists " + tableName + " (\n" +
        "c_tinyint TINYINT, \n" +
        "c_smallint SMALLINT, \n" +
        "c_int INT, \n" +
        "c_long BIGINT, \n" +
        "c_decimal DECIMAL(10,3), \n" +
        "c_date DATE, \n" +
        "c_time TIME, \n" +
        "c_timestamp TIMESTAMP, \n" +
        "c_timestamptz TIMESTAMP_LTZ, \n" +
        "c_string STRING, \n" +
        //            "c_uuid BINARY(16), \n" +
        //            "c_fixed BINARY(32), \n" +
        //            "c_binary VARBINARY(32), \n" +
        "p1 AS years(c_timestamp), \n" +
        "p2 AS days(c_date), \n" +
        "p3 AS buckets(4, c_date) \n" +
        ") PARTITIONED BY ( \n" +
        " p1, p2, p3, c_date " +
        ") with (" +
        "'write.format.default'='parquet'," +
        "'format-version'='2'" +
        ")");
    tEnv.executeSql("desc " + tableName).print();
  }

  @Test
  public void insertPartitionKeyTable() {
    tEnv.executeSql("insert into " + tableName + " values ( \n" +
        "9, " +
        "8, " +
        "1," +
        "2," +
        "3.4," +
        "DATE '2022-05-06', " +
        //            "TIME '14:08:59', "+
        "TO_TIMESTAMP(FROM_UNIXTIME(8))," +
        "TO_TIMESTAMP(FROM_UNIXTIME(9))," +
        "'string'" +
        //            "x'0102030405060708'," +
        //            "x'0a0b0c0d0e0f', " +
        //            "x'010203040506' " +
        ")").print();
    tEnv.executeSql("desc " + tableName).print();
  }

  @Test
  public void bucketPartition() {
    tEnv.sqlQuery("select " +
        "`buckets`(10, c_tinyint), " +
        "`buckets`(10, c_smallint), " +
        "`buckets`(10, c_int), " +
        "`buckets`(10, c_long), " +
        "`buckets`(10, c_decimal), " +
        "`buckets`(10, c_date), " +
        "`buckets`(10, c_time), " +
        "`buckets`(10, c_timestamp), " +
        "`buckets`(10, c_timestamptz), " +
        "`buckets`(10, c_string), " +
        "`buckets`(10, c_uuid), " +
        "`buckets`(10, c_fixed), " +
        "`buckets`(10, c_binary) " +
        "from " + tableName).execute().print();
  }

  @Test
  public void yearPartition() {
    tEnv.sqlQuery("select " +
        "`years`(c_date), " +
        "`years`(c_timestamp), " +
        "`years`(c_timestamptz) " +
        "from " + tableName).execute().print();
  }

  @Test
  public void teset() {
    tEnv.executeSql("INSERT into tb3 values (2)");
  }

  @Test
  public void monthPartition() {
    tEnv.sqlQuery("select " +
        "`months`(c_date), " +
        "`months`(c_timestamp), " +
        "`months`(c_timestamptz) " +
        "from " + tableName).execute().print();
  }

  @Test
  public void dayPartition() {
    tEnv.sqlQuery("select " +
        "`days`(c_date), " +
        "`days`(c_timestamp), " +
        "`days`(c_timestamptz) " +
        "from " + tableName).execute().print();
  }

  @Test
  public void hourPartition() {
    tEnv.sqlQuery("select " +
        "`hours`(c_timestamp), " +
        "`hours`(c_timestamptz) " +
        "from " + tableName).execute().print();
  }

  @Test
  public void truncatePartitionKeyTable() {
    tEnv.sqlQuery("select " +
            //            "abc.`truncate`(c_tinyint, 10), " +
            //            "abc.`truncate`(c_smallint, 10), " +
            //            "abc.`truncate`(c_int, 10), " +
            //            "abc.`truncate`(c_long, 10), " +
            //            "abc.`truncate`(c_decimal, 10) " +
            //            "abc.`truncate`(c_string, 2), " +
            //            "abc.`truncate`(c_uuid, 2) " +
            //            "abc.`truncate`(c_fixed, 10) " +
            //            "abc.`truncate`(c_binary, 2) " +
            "truncates(4, x'010203040506') " +
            ""
        //            "from " + tableName
    ).execute().print();
  }

  @Test
  public void getFunctions() {
    tEnv.executeSql("show functions").print();
  }

  @Test
  public void sqlExec() {
    //        tEnv.executeSql("desc " + tableName).print();
    tEnv.executeSql("alter table " + tableName + " set ('a'='b')").print();
  }

  @Test
  public void alterPartitions() {
    tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
    tEnv.executeSql("ALTER TABLE " + tableName + " \n" +
        "DROP PARTITION c_timestamp").print();
  }

  @Test
  public void testPPPP() {
    //        Collections.unmodifiableMap()
  }
}
