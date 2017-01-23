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
package com.rms.kuduworkshop

import java.util.Date

import com.google.common.collect.ImmutableList
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.client.MiniKuduCluster.MiniKuduClusterBuilder
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.{Schema, Type}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.util.Try

object SparkExample {

  var sc: SparkContext = null
  var kuduClient: KuduClient = null
  var kuduContext: KuduContext = null
  val kuduMaster: String = "127.0.0.1:64046"

  def main(s: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[*]")
    kuduContext = new KuduContext(kuduMaster)

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val addressDF = sqlContext.read.parquet("sampleData/structure=8944/address/address")
    println(addressDF.count)

    if (!kuduContext.tableExists("address")) {
      kuduContext.createTable("address", addressDF.schema, Seq("structureId","id","addressSchemeId"),  new CreateTableOptions().addHashPartitions(List("id").asJava, 10).setNumReplicas(1))
    }

    val options: Map[String, String] = Map(
      "kudu.table" -> "address",
      "kudu.master" -> kuduMaster)
    addressDF.write.options(options).mode("append").kudu

    val addressRead = sqlContext.read.options(options).kudu

    println(addressRead.count)

  }
}
