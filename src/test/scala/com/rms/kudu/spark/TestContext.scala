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


import java.io.File
import java.net.URL

import org.apache.kudu.spark.kudu._
import java.util.Date

import com.google.common.collect.ImmutableList
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.client.MiniKuduCluster.MiniKuduClusterBuilder
import org.apache.kudu.client._
import org.apache.kudu.{Schema, Type}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq

trait TestContext extends BeforeAndAfterAll { self: Suite =>

  var sc: SparkContext = null
  var miniCluster: MiniKuduCluster = null
  var kuduClient: KuduClient = null
  var table: KuduTable = null
  var kuduContext: KuduContext = null

  val tableName = "test"

  lazy val schema: Schema = {
    val columns = ImmutableList.of(
      new ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchemaBuilder("c1_i", Type.INT32).build(),
      new ColumnSchemaBuilder("c2_s", Type.STRING).nullable(true).build(),
      new ColumnSchemaBuilder("c3_double", Type.DOUBLE).build(),
      new ColumnSchemaBuilder("c4_long", Type.INT64).build(),
      new ColumnSchemaBuilder("c5_bool", Type.BOOL).build(),
      new ColumnSchemaBuilder("c6_short", Type.INT16).build(),
      new ColumnSchemaBuilder("c7_float", Type.FLOAT).build(),
      new ColumnSchemaBuilder("c8_binary", Type.BINARY).build(),
      new ColumnSchemaBuilder("c9_unixtime_micros", Type.UNIXTIME_MICROS).build(),
      new ColumnSchemaBuilder("c10_byte", Type.INT8).build())
    new Schema(columns)
  }

  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString

  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)

  override def beforeAll() {
    miniCluster = new MiniKuduClusterBuilder()
      .numMasters(1)
      .numTservers(1)
      .build()
    val envMap = Map[String,String](("Xmx", "512m"))

    sc = new SparkContext(conf)

    kuduClient = new KuduClientBuilder(miniCluster.getMasterAddresses).build()
    assert(miniCluster.waitForTabletServers(1))

    kuduContext = new KuduContext(miniCluster.getMasterAddresses)

    val tableOptions = new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
                                               .setNumReplicas(1)
    table = kuduClient.createTable(tableName, schema, tableOptions)
  }

  override def afterAll() {
    if (kuduClient != null) kuduClient.shutdown()
    if (miniCluster != null) miniCluster.shutdown()
    if (sc != null) sc.stop()
  }

  def deleteRow(key: Int): Unit = {
    val kuduSession = kuduClient.newSession()
    val delete = table.newDelete()
    delete.getRow.addInt(0, key)
    kuduSession.apply(delete)
  }

  def insertRows(rowCount: Integer): IndexedSeq[(Int, Int, String, Long)] = {
    val kuduSession = kuduClient.newSession()

    val rows = Range(0, rowCount).map { i =>
      val insert = table.newInsert
      val row = insert.getRow
      row.addInt(0, i)
      row.addInt(1, i)
      row.addDouble(3, i.toDouble)
      row.addLong(4, i.toLong)
      row.addBoolean(5, i%2==1)
      row.addShort(6, i.toShort)
      row.addFloat(7, i.toFloat)
      row.addBinary(8, s"bytes ${i}".getBytes())
      val ts = System.currentTimeMillis() * 1000
      row.addLong(9, ts)
      row.addByte(10, i.toByte)

      // Sprinkling some nulls so that queries see them.
      val s = if (i % 2 == 0) {
        row.addString(2, i.toString)
        i.toString
      } else {
        row.setNull(2)
        null
      }

      kuduSession.apply(insert)
      (i, i, s, ts)
    }
    rows
  }
}
