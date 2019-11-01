/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState, SessionStateBuilder, SQLConf}
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.types.StructType

/**
 * Test cases for the builder pattern of [[SparkSession]].
 */
class SparkSessionBuilderSuite extends SparkFunSuite with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    // This suite should not interfere with the other test suites.
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.getDefaultSession.foreach(_.stop())
    SparkSession.clearDefaultSession()
  }

  test("create with config options and propagate them to SparkContext and SparkSession") {
    val session = SparkSession.builder()
      .master("local")
      .config(UI_ENABLED.key, value = false)
      .config("some-config", "v2")
      .getOrCreate()
    assert(session.sparkContext.conf.get("some-config") == "v2")
    assert(session.conf.get("some-config") == "v2")
  }

  test("use global default session") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.builder().getOrCreate() == session)
  }

  test("sets default and active session") {
    assert(SparkSession.getDefaultSession == None)
    assert(SparkSession.getActiveSession == None)
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.getDefaultSession == Some(session))
    assert(SparkSession.getActiveSession == Some(session))
  }

  test("get active or default session") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.active == session)
    SparkSession.clearActiveSession()
    assert(SparkSession.active == session)
    SparkSession.clearDefaultSession()
    intercept[IllegalStateException](SparkSession.active)
    session.stop()
  }

  test("config options are propagated to existing SparkSession") {
    val session1 = SparkSession.builder().master("local").config("spark-config1", "a").getOrCreate()
    assert(session1.conf.get("spark-config1") == "a")
    val session2 = SparkSession.builder().config("spark-config1", "b").getOrCreate()
    assert(session1 == session2)
    assert(session1.conf.get("spark-config1") == "b")
  }

  test("use session from active thread session and propagate config options") {
    val defaultSession = SparkSession.builder().master("local").getOrCreate()
    val activeSession = defaultSession.newSession()
    SparkSession.setActiveSession(activeSession)
    val session = SparkSession.builder().config("spark-config2", "a").getOrCreate()

    assert(activeSession != defaultSession)
    assert(session == activeSession)
    assert(session.conf.get("spark-config2") == "a")
    assert(session.sessionState.conf == SQLConf.get)
    assert(SQLConf.get.getConfString("spark-config2") == "a")
    SparkSession.clearActiveSession()

    assert(SparkSession.builder().getOrCreate() == defaultSession)
  }

  test("create a new session if the default session has been stopped") {
    val defaultSession = SparkSession.builder().master("local").getOrCreate()
    SparkSession.setDefaultSession(defaultSession)
    defaultSession.stop()
    val newSession = SparkSession.builder().master("local").getOrCreate()
    assert(newSession != defaultSession)
  }

  test("create a new session if the active thread session has been stopped") {
    val activeSession = SparkSession.builder().master("local").getOrCreate()
    SparkSession.setActiveSession(activeSession)
    activeSession.stop()
    val newSession = SparkSession.builder().master("local").getOrCreate()
    assert(newSession != activeSession)
  }

  test("create SparkContext first then SparkSession") {
    val conf = new SparkConf().setAppName("test").setMaster("local").set("key1", "value1")
    val sparkContext2 = new SparkContext(conf)
    val session = SparkSession.builder().config("key2", "value2").getOrCreate()
    assert(session.conf.get("key1") == "value1")
    assert(session.conf.get("key2") == "value2")
    assert(session.sparkContext == sparkContext2)
    // We won't update conf for existing `SparkContext`
    assert(!sparkContext2.conf.contains("key2"))
    assert(sparkContext2.conf.get("key1") == "value1")
  }

  test("create SparkContext first then pass context to SparkSession") {
    val conf = new SparkConf().setAppName("test").setMaster("local").set("key1", "value1")
    val newSC = new SparkContext(conf)
    val session = SparkSession.builder().sparkContext(newSC).config("key2", "value2").getOrCreate()
    assert(session.conf.get("key1") == "value1")
    assert(session.conf.get("key2") == "value2")
    assert(session.sparkContext == newSC)
    assert(session.sparkContext.conf.get("key1") == "value1")
    // If the created sparkContext is passed through the Builder's API sparkContext,
    // the conf of this sparkContext will not contain the conf set through the API config.
    assert(!session.sparkContext.conf.contains("key2"))
    assert(session.sparkContext.conf.get("spark.app.name") == "test")
  }

  test("SPARK-15887: hive-site.xml should be loaded") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(session.sessionState.newHadoopConf().get("hive.in.test") == "true")
    assert(session.sparkContext.hadoopConfiguration.get("hive.in.test") == "true")
  }

  test("SPARK-15991: Set global Hadoop conf") {
    val session = SparkSession.builder().master("local").getOrCreate()
    val mySpecialKey = "my.special.key.15991"
    val mySpecialValue = "msv"
    try {
      session.sparkContext.hadoopConfiguration.set(mySpecialKey, mySpecialValue)
      assert(session.sessionState.newHadoopConf().get(mySpecialKey) == mySpecialValue)
    } finally {
      session.sparkContext.hadoopConfiguration.unset(mySpecialKey)
    }
  }

  test("SPARK-31234: RESET command will not change static sql configs and " +
    "spark context conf values in SessionState") {
    val session = SparkSession.builder()
      .master("local")
      .config(GLOBAL_TEMP_DATABASE.key, value = "globalTempDB-SPARK-31234")
      .config("spark.app.name", "test-app-SPARK-31234")
      .getOrCreate()

    assert(session.sessionState.conf.getConfString("spark.app.name") === "test-app-SPARK-31234")
    assert(session.sessionState.conf.getConf(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31234")
    session.sql("RESET")
    assert(session.sessionState.conf.getConfString("spark.app.name") === "test-app-SPARK-31234")
    assert(session.sessionState.conf.getConf(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31234")
  }

  test("SPARK-31354: SparkContext only register one SparkSession ApplicationEnd listener") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-app-SPARK-31354-1")
    val context = new SparkContext(conf)
    SparkSession
      .builder()
      .sparkContext(context)
      .master("local")
      .getOrCreate()
    val postFirstCreation = context.listenerBus.listeners.size()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    SparkSession
      .builder()
      .sparkContext(context)
      .master("local")
      .getOrCreate()
    val postSecondCreation = context.listenerBus.listeners.size()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    assert(postFirstCreation == postSecondCreation)
  }

  test("SPARK-31532: should not propagate static sql configs to the existing" +
    " active/default SparkSession") {
    val session = SparkSession.builder()
      .master("local")
      .config(GLOBAL_TEMP_DATABASE.key, value = "globalTempDB-SPARK-31532")
      .config("spark.app.name", "test-app-SPARK-31532")
      .getOrCreate()
    // do not propagate static sql configs to the existing active session
    val session1 = SparkSession
      .builder()
      .config(GLOBAL_TEMP_DATABASE.key, "globalTempDB-SPARK-31532-1")
      .getOrCreate()
    assert(session.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532")
    assert(session1.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532")

    // do not propagate static sql configs to the existing default session
    SparkSession.clearActiveSession()
    val session2 = SparkSession
      .builder()
      .config(WAREHOUSE_PATH.key, "SPARK-31532-db")
      .config(GLOBAL_TEMP_DATABASE.key, value = "globalTempDB-SPARK-31532-2")
      .getOrCreate()

    assert(!session.conf.get(WAREHOUSE_PATH).contains("SPARK-31532-db"))
    assert(session.conf.get(WAREHOUSE_PATH) === session2.conf.get(WAREHOUSE_PATH))
    assert(session2.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532")
  }

  test("SPARK-31532: propagate static sql configs if no existing SparkSession") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-app-SPARK-31532-2")
      .set(GLOBAL_TEMP_DATABASE.key, "globaltempdb-spark-31532")
      .set(WAREHOUSE_PATH.key, "SPARK-31532-db")
    SparkContext.getOrCreate(conf)

    // propagate static sql configs if no existing session
    val session = SparkSession
      .builder()
      .config(GLOBAL_TEMP_DATABASE.key, "globalTempDB-SPARK-31532-2")
      .config(WAREHOUSE_PATH.key, "SPARK-31532-db-2")
      .getOrCreate()
    assert(session.conf.get("spark.app.name") === "test-app-SPARK-31532-2")
    assert(session.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532-2")
    assert(session.conf.get(WAREHOUSE_PATH) === "SPARK-31532-db-2")
  }

  test("SPARK-17767: Spark SQL ExternalCatalog API custom implementation support") {
    val session = SparkSession.builder()
      .master("local")
      .enableProvidedCatalog()
      .config("spark.sql.externalCatalog", "org.apache.spark.sql.MyExternalCatalog")
      .config("spark.sql.sessionStateBuilder", "org.apache.spark.sql.MySessionStateBuilder")
      .getOrCreate()
    assert(session.sharedState.externalCatalog.unwrapped.isInstanceOf[MyExternalCatalog])
    assert(session.sessionState.conf.getConfString("MySessionStateBuilder") == "true")
  }

  test("SPARK-17767 - Fail on missing configs") {
    val session = SparkSession.builder()
      .master("local")
      .enableProvidedCatalog()
      .getOrCreate()
    assertThrows[SparkException](session.sharedState.externalCatalog)
    assertThrows[SparkException](session.sessionState)
  }
}

class MyExternalCatalog(conf: SparkConf, hadoopConf: Configuration) extends ExternalCatalog {

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {}
  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {}
  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {}
  override def getDatabase(db: String): CatalogDatabase = null
  override def databaseExists(db: String): Boolean = true
  override def listDatabases(): Seq[String] = Seq.empty
  override def listDatabases(pattern: String): Seq[String] = Seq.empty
  override def setCurrentDatabase(db: String): Unit = {}
  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {}
  override def dropTable(db: String,
                         table: String,
                         ignoreIfNotExists: Boolean,
                         purge: Boolean): Unit = {}
  override def renameTable(db: String, oldName: String, newName: String): Unit = {}
  override def alterTable(tableDefinition: CatalogTable): Unit = {}
  override def alterTableDataSchema(db: String, table: String,
                                    newDataSchema: StructType): Unit = {}
  override def alterTableStats(db: String,
                               table: String,
                               stats: Option[CatalogStatistics]): Unit = {}
  override def getTable(db: String, table: String): CatalogTable = null
  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = Seq.empty
  override def tableExists(db: String, table: String): Boolean = true
  override def listTables(db: String): Seq[String] = Seq.empty
  override def listTables(db: String, pattern: String): Seq[String] = Seq.empty
  override def loadTable(db: String, table: String, loadPath: String,
                         isOverwrite: Boolean, isSrcLocal: Boolean): Unit = {}
  override def loadPartition(db: String, table: String, loadPath: String,
                             partition: TablePartitionSpec,
                             isOverwrite: Boolean, inheritTableSpecs: Boolean,
                             isSrcLocal: Boolean): Unit = {}
  override def loadDynamicPartitions(db: String, table: String, loadPath: String,
                                     partition: TablePartitionSpec,
                                     replace: Boolean, numDP: Int): Unit = {}
  override def createPartitions(db: String, table: String,
                                parts: Seq[CatalogTablePartition],
                                ignoreIfExists: Boolean): Unit = {}
  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec],
                              ignoreIfNotExists: Boolean,
                              purge: Boolean, retainData: Boolean): Unit = {}
  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec],
                                newSpecs: Seq[TablePartitionSpec]): Unit = {}
  override def alterPartitions(db: String, table: String,
                               parts: Seq[CatalogTablePartition]): Unit = {}
  override def getPartition(db: String, table: String,
                            spec: TablePartitionSpec): CatalogTablePartition = null
  override def getPartitionOption(db: String, table: String,
                                  spec: TablePartitionSpec):
  Option[CatalogTablePartition] = None
  override def listPartitionNames(db: String, table: String,
                                  partialSpec: Option[TablePartitionSpec]):
  Seq[String] = Seq.empty
  override def listPartitions(db: String, table: String,
                              partialSpec: Option[TablePartitionSpec]):
  Seq[CatalogTablePartition] = Seq.empty
  override def listPartitionsByFilter(db: String, table: String,
                                      predicates: Seq[Expression],
                                      defaultTimeZoneId: String):
  Seq[CatalogTablePartition] = Seq.empty
  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = {}
  override def dropFunction(db: String, funcName: String): Unit = {}
  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = {}
  override def renameFunction(db: String, oldName: String, newName: String): Unit = {}
  override def getFunction(db: String, funcName: String): CatalogFunction = null
  override def functionExists(db: String, funcName: String): Boolean = true
  override def listFunctions(db: String, pattern: String): Seq[String] = Seq.empty
  override def listViews(db: String, pattern: String): Seq[String] = Seq.empty
}

class MySessionStateBuilder(session: SparkSession,
                            parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(session, parentState) {
  override protected def newBuilder: NewBuilder = {
    conf.setConfString("MySessionStateBuilder", "true")
    new SessionStateBuilder(_, _)
  }
}
