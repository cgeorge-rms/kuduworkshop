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
package com.rms.kudu.spark


import org.apache.kudu.spark.kudu._

import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.util.control.NonFatal;

@RunWith(classOf[JUnitRunner])
class DefaultSourceTest extends FunSuite with TestContext with BeforeAndAfter {


  val rowCount = 10
  var sqlContext : SQLContext = _
  var rows : IndexedSeq[(Int, Int, String, Long)] = _
  var kuduOptions : Map[String, String] = _

  before {
    val rowCount = 10
    rows = insertRows(rowCount)

    sqlContext = new SQLContext(sc)

    kuduOptions = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> miniCluster.getMasterAddresses)

    sqlContext.read.options(kuduOptions).kudu.registerTempTable(tableName)
  }

  test("table creation") {
    val tableName = "testcreatetable"
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
    val df = sqlContext.read.options(kuduOptions).kudu
    kuduContext.createTable(tableName, df.schema, Seq("key"),
                            new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
                                                    .setNumReplicas(1))
    kuduContext.insertRows(df, tableName)

    // now use new options to refer to the new table name
    val newOptions: Map[String, String] = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> miniCluster.getMasterAddresses)
    val checkDf = sqlContext.read.options(newOptions).kudu

    assert(checkDf.schema === df.schema)
    assertTrue(kuduContext.tableExists(tableName))
    assert(checkDf.count == 10)

    kuduContext.deleteTable(tableName)
    assertFalse(kuduContext.tableExists(tableName))
  }

  test("insertion") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val changedDF = df.limit(1).withColumn("key", df("key").plus(100)).withColumn("c2_s", lit("abc"))
    kuduContext.insertRows(changedDF, tableName)

    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collected = newDF.filter("key = 100").collect()
    assertEquals("abc", collected(0).getAs[String]("c2_s"))

    deleteRow(100)
  }

  test("insertion multiple") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val changedDF = df.limit(2).withColumn("key", df("key").plus(100)).withColumn("c2_s", lit("abc"))
    kuduContext.insertRows(changedDF, tableName)

    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collected = newDF.filter("key = 100").collect()
    assertEquals("abc", collected(0).getAs[String]("c2_s"))

    val collectedTwo = newDF.filter("key = 101").collect()
    assertEquals("abc", collectedTwo(0).getAs[String]("c2_s"))

    deleteRow(100)
    deleteRow(101)
  }

  test("insert ignore rows") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val baseDF = df.limit(1) // filter down to just the first row

    // change the c2 string to abc and insert
    val updateDF = baseDF.withColumn("c2_s", lit("abc"))
    kuduContext.insertIgnoreRows(updateDF, tableName)

    // change the key and insert
    val insertDF = df.limit(1).withColumn("key", df("key").plus(100)).withColumn("c2_s", lit("def"))
    kuduContext.insertIgnoreRows(insertDF, tableName)

    // read the data back
    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("0", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))

    // restore the original state of the table
    deleteRow(100)
  }

  test("upsert rows") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val baseDF = df.limit(1) // filter down to just the first row

    // change the c2 string to abc and update
    val updateDF = baseDF.withColumn("c2_s", lit("abc"))
    kuduContext.upsertRows(updateDF, tableName)

    // change the key and insert
    val insertDF = df.limit(1).withColumn("key", df("key").plus(100)).withColumn("c2_s", lit("def"))
    kuduContext.upsertRows(insertDF, tableName)

    // read the data back
    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collectedUpdate = newDF.filter("key = 0").collect()
    assertEquals("abc", collectedUpdate(0).getAs[String]("c2_s"))
    val collectedInsert = newDF.filter("key = 100").collect()
    assertEquals("def", collectedInsert(0).getAs[String]("c2_s"))

    // restore the original state of the table
    kuduContext.updateRows(baseDF.filter("key = 0").withColumn("c2_s", lit("0")), tableName)
    deleteRow(100)
  }

  test("delete rows") {
    val df = sqlContext.read.options(kuduOptions).kudu
    val deleteDF = df.filter("key = 0").select("key")
    kuduContext.deleteRows(deleteDF, tableName)

    // read the data back
    val newDF = sqlContext.read.options(kuduOptions).kudu
    val collectedDelete = newDF.filter("key = 0").collect()
    assertEquals(0, collectedDelete.length)

    // restore the original state of the table
    val insertDF = df.limit(1).filter("key = 0")
    kuduContext.insertRows(insertDF, tableName)
  }

  test("out of order selection") {
    val df = sqlContext.read.options(kuduOptions).kudu.select( "c2_s", "c1_i", "key")
    val collected = df.collect()
    assert(collected(0).getString(0).equals("0"))

  }

  test("table scan") {
    val results = sqlContext.sql(s"SELECT * FROM $tableName").collectAsList()
    assert(results.size() == rowCount)

    assert(!results.get(0).isNullAt(2))
    assert(results.get(1).isNullAt(2))
  }

  test("table scan with projection") {
    assertEquals(10, sqlContext.sql(s"""SELECT key FROM $tableName""").count())
  }

  test("table scan with projection and predicate double") {
    assertEquals(rows.count { case (key, i, s, ts) => i > 5 },
                 sqlContext.sql(s"""SELECT key, c3_double FROM $tableName where c3_double > "5.0"""").count())
  }

  test("table scan with projection and predicate long") {
    assertEquals(rows.count { case (key, i, s, ts) => i > 5 },
                 sqlContext.sql(s"""SELECT key, c4_long FROM $tableName where c4_long > "5"""").count())

  }
  test("table scan with projection and predicate bool") {
    assertEquals(rows.count { case (key, i, s, ts) => i % 2==0 },
                 sqlContext.sql(s"""SELECT key, c5_bool FROM $tableName where c5_bool = true""").count())

  }
  test("table scan with projection and predicate short") {
    assertEquals(rows.count { case (key, i, s, ts) => i > 5},
                 sqlContext.sql(s"""SELECT key, c6_short FROM $tableName where c6_short > 5""").count())

  }
  test("table scan with projection and predicate float") {
    assertEquals(rows.count { case (key, i, s, ts) => i > 5},
                 sqlContext.sql(s"""SELECT key, c7_float FROM $tableName where c7_float > 5""").count())

  }

  test("table scan with projection and predicate ") {
    assertEquals(rows.count { case (key, i, s, ts) => s != null && s > "5" },
      sqlContext.sql(s"""SELECT key FROM $tableName where c2_s > "5"""").count())

    assertEquals(rows.count { case (key, i, s, ts) => s != null },
      sqlContext.sql(s"""SELECT key, c2_s FROM $tableName where c2_s IS NOT NULL""").count())
  }


  test("Test basic SparkSQL") {
    val results = sqlContext.sql("SELECT * FROM " + tableName).collectAsList()
    assert(results.size() == rowCount)

    assert(results.get(1).isNullAt(2))
    assert(!results.get(0).isNullAt(2))
  }

  test("Test basic SparkSQL projection") {
    val results = sqlContext.sql("SELECT key FROM " + tableName).collectAsList()
    assert(results.size() == rowCount)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(0))
  }

  test("Test basic SparkSQL with predicate") {
    val results = sqlContext.sql("SELECT key FROM " + tableName + " where key=1").collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(1))

  }

  test("Test basic SparkSQL with two predicates") {
    val results = sqlContext.sql("SELECT key FROM " + tableName + " where key=2 and c2_s='2'").collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(2))
  }

  test("Test basic SparkSQL with two predicates negative") {
    val results = sqlContext.sql("SELECT key FROM " + tableName + " where key=1 and c2_s='2'").collectAsList()
    assert(results.size() == 0)
  }

  test("Test basic SparkSQL with two predicates including string") {
    val results = sqlContext.sql("SELECT key FROM " + tableName + " where c2_s='2'").collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(1))
    assert(results.get(0).getInt(0).equals(2))
  }

  test("Test basic SparkSQL with two predicates and projection") {
    val results = sqlContext.sql("SELECT key, c2_s FROM " + tableName + " where c2_s='2'").collectAsList()
    assert(results.size() == 1)
    assert(results.get(0).size.equals(2))
    assert(results.get(0).getInt(0).equals(2))
    assert(results.get(0).getString(1).equals("2"))
  }

  test("Test basic SparkSQL with two predicates greater than") {
    val results = sqlContext.sql("SELECT key, c2_s FROM " + tableName + " where c2_s>='2'").collectAsList()
    assert(results.size() == 4)
    assert(results.get(0).size.equals(2))
    assert(results.get(0).getInt(0).equals(2))
    assert(results.get(0).getString(1).equals("2"))
  }

  test("Test SQL: insert into") {
    val insertTable = "insertintotest"

    // read 0 rows just to get the schema
    val df = sqlContext.sql(s"SELECT * FROM $tableName LIMIT 0")
    kuduContext.createTable(insertTable, df.schema, Seq("key"),
      new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
        .setNumReplicas(1))

    val newOptions: Map[String, String] = Map(
      "kudu.table" -> insertTable,
      "kudu.master" -> miniCluster.getMasterAddresses)
    sqlContext.read.options(newOptions).kudu.registerTempTable(insertTable)

    sqlContext.sql(s"INSERT INTO TABLE $insertTable SELECT * FROM $tableName")
    val results = sqlContext.sql(s"SELECT key FROM $insertTable").collectAsList()
    assertEquals(10, results.size())
  }

  test("Test SQL: insert overwrite unsupported") {
    val insertTable = "insertoverwritetest"

    // read 0 rows just to get the schema
    val df = sqlContext.sql(s"SELECT * FROM $tableName LIMIT 0")
    kuduContext.createTable(insertTable, df.schema, Seq("key"),
      new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
        .setNumReplicas(1))

    val newOptions: Map[String, String] = Map(
      "kudu.table" -> insertTable,
      "kudu.master" -> miniCluster.getMasterAddresses)
    sqlContext.read.options(newOptions).kudu.registerTempTable(insertTable)

    try {
      sqlContext.sql(s"INSERT OVERWRITE TABLE $insertTable SELECT * FROM $tableName")
      fail()
    } catch {
      case _: UnsupportedOperationException => // good
      case NonFatal(_) => fail()
    }
  }

  test("Test write using DefaultSource") {
    val df = sqlContext.read.options(kuduOptions).kudu

    val newTable = "testwritedatasourcetable"
    kuduContext.createTable(newTable, df.schema, Seq("key"),
        new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
          .setNumReplicas(1))

    val newOptions: Map[String, String] = Map(
      "kudu.table" -> newTable,
      "kudu.master" -> miniCluster.getMasterAddresses)
    df.write.options(newOptions).mode("append").kudu

    val checkDf = sqlContext.read.options(newOptions).kudu
    assert(checkDf.schema === df.schema)
    assertTrue(kuduContext.tableExists(newTable))
    assert(checkDf.count == 10)
  }
}
