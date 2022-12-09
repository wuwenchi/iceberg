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

package org.apache.iceberg.spark.actions;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.BinPackStrategy;
//import org.apache.iceberg.actions.CurveOptimizeStrategy;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
// import org.apache.spark.sql.execution.datasources.RangeSampleSort$;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.actions.BinPackStrategy.MIN_INPUT_FILES;

public class SparkOpt {
  static String dbName;
  static String tableName;
  static Catalog catalog;
  static HiveCatalog hiveCatalog;
  static TableIdentifier tableId;
  static String hdfs;
  static String uri;
  static String localPath;
  static Table table;
  static SparkSession sparkSession;
  static String tableLocation;
  static String catalogName;
  static int inputCnt;
  static int inputStep;
  static boolean drop;
  static int parallelism;
  static int method;

  @Before
  public void envv() {

    System.setProperty("HADOOP_USER_NAME", "root");
    parameters();
    tableId = TableIdentifier.of(dbName, tableName);

    if (method == 1) {
      tableLocation = localPath + "/" + dbName + "/" + tableName;
      createHadoopCatalog();
    } else {
      createHiveCatalog();
    }
    //    createSparkSession();
    //    sparkSession.conf().set("spark.network.timeout", "360000");
    //    sparkSession.conf().set("spark.rpc.askTimeout", "360000");
    //    sparkSession.conf().set("spark.driver.maxResultSize", "4g");
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
    tableName = "tb2";
    //        dbName = "aaa";
    //        tableName = "account_orc222";
    //        tableName = "cx_01";
    inputCnt = 4;
    inputStep = 1;
    drop = true;
    localPath = "file:///Users/wuwenchi/github/iceberg/warehouse";
    method = 1;

    System.out.println("===================================================");
    System.out.println(System.getProperty("user.dir"));
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
  public void sparkAllOpt() throws NoSuchTableException {
    createTable();
    writeTable();
  }

  @Test
  public void testEmp() {
    System.out.println("dfdfdf");
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
    //    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    //    hiveCatalog.listNamespaces().forEach(System.out::println);
    //    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    //    catalog.listTables(Namespace.of(dbName)).forEach(System.out::println);
    //    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

    table = catalog.loadTable(tableId);
  }

  public void createSparkSession() {
    System.setProperty("HADOOP_USER_NAME", "root");

    sparkSession = SparkSession.builder()
        .master("local[5]")
        //        .master("spark://10.0.30.12:35090")
        .appName("sparkApp")
        //              .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("hive.metastore.uris", uri)
        .config("spark.hive.metastore.uris", uri)
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.uri", uri)
        .config("spark.sql.catalog.spark_catalog.warehouse", hdfs)

        .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive.type", "hive")
        .config("spark.sql.catalog.hive.uri", uri)
        .config("spark.sql.catalog.hive.warehouse", hdfs)

        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", localPath)

        .enableHiveSupport()
        .getOrCreate();
    if (method == 1) {
      sparkSession.sql("use local");
    } else if (method == 3) {
      sparkSession.sql("use hive");
    }
    sparkSession.sql("use " + dbName);
  }

  @After
  public void closeSpark() {
    //    sparkSession.close();
  }

  @Test
  public void updatePor() {
    table.updateProperties().set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "512*1024").commit();
  }

  @Test
  public void createHadoopCatalog() {
    catalog = new HadoopCatalog(new Configuration(), localPath);
    //    table = catalog.loadTable(tableId);
  }

  @Test
  public void createTable() {
    //    new HadoopTables(new Configuration());
    if (catalog.tableExists(tableId)) {
      catalog.dropTable(tableId);
    }
    //创建表
    List<Types.NestedField> columns = new ArrayList<>();
    columns.add(Types.NestedField.optional(11, "c1", Types.IntegerType.get()));
    columns.add(Types.NestedField.optional(12, "c2", Types.StringType.get()));
    columns.add(Types.NestedField.optional(13, "c3", Types.StringType.get()));

    HashSet<Integer> identifierFieldIds = new HashSet<>();
    //    identifierFieldIds.add(11);
    Schema schema = new Schema(columns, identifierFieldIds);
    //    Schema schema = new Schema(columns);

    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
        //            .identity("c2")
        .bucket("c2", 2)
        .build();

    Map<String, String> tableProperties = Maps.newHashMap();
    //        tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true"); // engine.hive.enabled=true
    //    tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "orc");
    //    tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
    tableProperties.put(TableProperties.FORMAT_VERSION, "2");//v2 版本才支持delete file
    //    tableProperties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(64 * 1024));
    tableProperties.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(10 * 1024 * 1024));
    //    tableProperties.put(TableProperties.PARQUET_PAGE_SIZE_BYTES, String.valueOf(128*1024));
    //    tableProperties.put(TableProperties.UPSERT_ENABLED, "true");

    catalog.createTable(tableId, schema, partitionSpec, tableProperties);
    Table table = catalog.loadTable(tableId);
    System.out.println("ok");
  }

  @Test
  public void createTableSql() {
    sparkSession.sql("create table " + tableName + " (  \n" +
        "id bigint, \n" +
        "data string, \n" +
        "ts timestamp) \n" +
        "using iceberg \n" +
        "PARTITIONED BY (days(ts), bucket(2, id))  \n" +
        "TBLPROPERTIES ('format-version'='2')");

    sparkSession.sql("desc " + tableName);
    sparkSession.sql("ALTER TABLE " + tableName + " SET TBLPROPERTIES (\n" +
        "    'comment' = 'A table comment.'\n" +
        ")");
  }

  @Test
  public void dropTableSql() {
    sparkSession.sql("use spark_catalog.abc;");
    //    sparkSession.sql("create table " + tableName + " (id int) using iceberg");
    sparkSession.sql("drop table tb2 purge");
    System.out.println("sdfdf");
  }

  @Test
  public void showDatabases() {
    sparkSession.sql("use hive");
    sparkSession.sql("show databases;").show();
    hiveCatalog.listNamespaces().forEach(System.out::println);
  }

  @Test
  public void writeSql() {
    sparkSession.sql("insert into " + tableName + " values(1, 'a', 'a')");
    sparkSession.sql("insert into " + tableName + " values(2, 'b', 'a')");
    sparkSession.sql("insert into " + tableName + " values(3, 'c', 'a')");
    sparkSession.sql("insert into " + tableName + " values(4, 'd', 'a')");
  }

  @Test
  public void writeTable() throws NoSuchTableException {
    for (int j = 0; j < inputCnt; j++) {
      List<ThreeColumnRecord> records = Lists.newArrayList();
      for (int i = 0; i < inputStep; i++) {
        String s = "_insert";
        records.add(new ThreeColumnRecord(
            j * inputStep + i,
            RandomStringUtils.randomAlphabetic(5) + s,
            RandomStringUtils.randomAlphabetic(5) + s
        ));
      }
      Dataset<Row> ds = sparkSession.createDataFrame(records, ThreeColumnRecord.class).repartition(1);
      //      ds.select("c1","c2","c3")
      //              .write()
      //              .format("iceberg")
      //              .mode("append")
      //              .save(tableLocation);
      ds
          //          .sortWithinPartitions("c2")
          .writeTo("local." + dbName + "." + tableName)
          .append();
    }
  }

  @Test
  public void binPack() {
    Table table = new HadoopTables(new Configuration()).load(tableLocation);
    //    Table table = catalog.loadTable(tableId);
    RewriteDataFiles.Result result = SparkActions.get().rewriteDataFiles(table)
        .option(MIN_INPUT_FILES, "1")
        .option(BinPackStrategy.DELETE_FILE_THRESHOLD, Integer.toString(1))
        //            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Integer.toString(1040*4))
        .option(RewriteDataFiles.USE_STARTING_SEQUENCE_NUMBER, Boolean.toString(true))
        .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, Boolean.toString(true))
        //            .option(BinPackStrategy.MIN_FILE_SIZE_BYTES, Integer.toString(1060))
        //            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
        //            .option()
        .execute();
    System.out.println(result.rewriteResults());
    System.out.println(result.rewrittenDataFilesCount());
    System.out.println(result.addedDataFilesCount());
  }

  @Test
  public void look() {
    sparkSession.sql("select * from local.iceberg_db.tb3").show();
  }

  @Test
  public void zorder() throws AnalysisException {

    long old = System.currentTimeMillis();
    //    org.apache.spark.sql.catalog.Table table = sparkSession.catalog().getTable("local." + dbName + "." + tableName);

    SparkActions.get(sparkSession)
        .rewriteDataFiles(Spark3Util.loadIcebergTable(sparkSession, tableName))
        .zOrder("c2", "c3")
        .option(MIN_INPUT_FILES, "1")
        //        .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, Boolean.toString(true))
        .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Integer.toString(45 * 1024 * 1024))
        //        .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, String.valueOf(1))
        .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, String.valueOf(5))
        //            .option(RewriteDataFiles.END_SNAPSHOT_ID, String.valueOf(34))
        .execute();

    //    SparkActions.get().rewriteDataFiles(table)
    //                    .option(RewriteDataFiles.END_SNAPSHOT_ID, "1234567789")
    //                            .execute();

    System.out.println((System.currentTimeMillis() - old) / 1000 / 60 + 1);
  }

  @Test
  public void createPartitionKeyTable() {
    sparkSession.sql("drop table if exists " + tableName);
    //    sparkSession.sql("create table if not exists " + tableName + " (\n" +
    //        "c_int int, \n" +
    //        "c_long long, \n" +
    //        "c_decimal Decimal(10,3), \n" +
    //        "c_date date, \n" +
    ////        "c_time TIME, \n" +
    //        "c_timestamp Timestamp, \n" +
    //        "c_string STRING, \n" +
    //        "c_uuid String, \n" +
    //        "c_fixed Binary, \n" +
    //        "c_binary Binary \n" +
    //        ")");
    sparkSession.sql("create table if not exists " + tableName + " (\n" +
        "c_int int, \n" +
        "c_date date \n" +
        //        "c_time TIME, \n" +
        ") using iceberg");
  }

  @Test
  public void insertPartitionKeyTable() {
    sparkSession.sql("desc " + tableName).show();
    //    sparkSession.sql("insert into " + tableName + " values ( \n" +
    //        "9, \n" +
    //        "8, \n" +
    //        "3.4, \n" +
    //        "to_date('2009-07-30 04:17:52'), \n" +
    ////        "'2009-07-30', " +
    //        "to_timestamp('2016-12-31 00:12:00'), \n" +
    //        "'string', \n" +
    //        "x'12345678', \n" +
    //        "x'abcdef', \n" +
    //        "x'87654321' \n" +
    //        ")").show();
    sparkSession.sql("insert into tb1 values (to_date('2009-07-30'))").show();
  }

  public static class TwoColumns {
    private Integer c1;
    private DateType c2;

    public TwoColumns() {
    }

    public TwoColumns(Integer c1, DateType c2) {
      this.c1 = c1;
      this.c2 = c2;
    }
  }

  public static class Tuple<X, Y> implements Serializable {
    public final X x;
    public final Y y;

    public Tuple(X x, Y y) {
      this.x = x;
      this.y = y;
    }
  }

  @Test
  public void ttt() {
    Dataset<Long> range = sparkSession.range(10);
    //    range.collectAsList().forEach(System.out::println);
    //    List<Long> longs = range.collectAsList();
    //    for (Long aLong : longs) {
    //      System.out.println(aLong);
    //    }
    //    Dataset<Row> rowDataset = range.map(it -> {
    //      return new Tuple<Integer, Integer>(
    //          (int) (Math.random() * 100000),
    //          (int) (Math.random() * 100000)
    //      );
    //    }).toDF("c1, c2");
  }

  static public void src(Dataset<Row> df, String[] zOrderFields) {
    SparkSession sparkSession = df.sparkSession();
    RDD<Row> rdd = df.rdd();
    HashMap<String, StructField> objectObjectHashMap = Maps.newHashMap();
    Arrays.stream(df.schema().fields()).forEach(field -> objectObjectHashMap.put(field.name(), field));

    // 检查类型？
    //    Arrays.stream(zOrderFields).map(field -> {
    //
    //    })
  }

  @Test
  public void aaaain() {
    // configure spark
    SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD")
        .setMaster("local[2]").set("spark.executor.memory", "2g");
    // start a spark context
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // initialize an integer RDD
    JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(14, 21, 88, 99, 455));

    // map each line to number of words in the line
    //    JavaRDD<Double> log_values = numbers.map(x -> Math.log(x));
    JavaRDD<Tuple<Integer, Integer>> map = numbers.map(x -> new Tuple<>(
        (int) (Math.random() * 100000),
        (int) (Math.random() * 100000)
    ));

    // collect RDD for printing
    for (Tuple<Integer, Integer> integerIntegerTuple : map.collect()) {
      System.out.println(integerIntegerTuple.x);
      System.out.println(integerIntegerTuple.y);
    }
  }

  @Test
  public void zzzzz() {
    ArrayList<String> c1 = Lists.newArrayList("c1");
    //    List<StructField> zOrderColumns = c1.stream().map(scanDF.schema()::apply).collect(Collectors.toList());
    SparkZOrderUDF zOrderUDF = new SparkZOrderUDF(c1.size(), 3, 300);
  }
}
