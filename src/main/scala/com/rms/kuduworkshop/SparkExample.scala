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

import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.{Schema, Type}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConverters._

/*
This is an example of creating a table with the same schema as an existing parquet file
Then it loads the parquet file into the kudu table
and then counting the number of rows in that table
It also shows an example of a pushdown predicate and pushdown filter

Running this requires the user to run kudubinary/threeNodeCluster.sh

 */
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

    val options: Map[String, String] = Map(
      "kudu.table" -> "address",
      "kudu.master" -> kuduMaster)

    if (!kuduContext.tableExists("address")) {
      //read the parquet file
      val addressDF = sqlContext.read.parquet("sampleData/structure=8944/address/address")
      //print the number of lines in parquet file
      println("Parquet file contains this many lines:"+addressDF.count)
      //creates a table matching the parquet file with a composite primary key Seq("structureId","id","addressSchemeId") and hash partitioned based on Id with 10 total partitions. For High Availability it has 2 replicas for each partition
      kuduContext.createTable("address", addressDF.schema, Seq("structureId","id","addressSchemeId"),  new CreateTableOptions().addHashPartitions(List("id").asJava, 10).setNumReplicas(3))

      addressDF.write.options(options).mode("append").kudu
    }

    val addressRead = sqlContext.read.options(options).kudu

    //show the first few rows of the table to know that we have something there
    println("Show on address table")
    addressRead.show
    //print the count of the table
    println("Line count on address table")
    println(addressRead.count)

    // simple filter with a projection
    val dfWithFilterAndProjection = addressRead.filter("externalId='326686'").select("id", "externalId")

    /*
    You can see with the explain that the pushed filters has an equal ([EqualTo(externalId,326686)]). As well as the projection [id#1L,externalId#4]
    This means that spark is smart enough to have kudu handle the projection and the filter
    == Physical Plan ==
Scan org.apache.kudu.spark.kudu.KuduRelation@113dcaf8[id#1L,externalId#4] PushedFilters: [EqualTo(externalId,326686)]
     */
    println("Filter with projection (pushdown) explain plan")
    dfWithFilterAndProjection.explain(true)

    /*
    Notice the following compared to the above explain... it has a lower case on the kudu column.
    Since Kudu cannot do server side lowercase comparisons the physical plan will not contain the filter.
    On a large table this would be a disaster as spark will pull down the entire table to satisfy the filter
    == Physical Plan ==
      Filter (lower(externalId#4) = 326686)
      +- Scan org.apache.kudu.spark.kudu.KuduRelation@113dcaf8[id#1L,externalId#4]

     */
    println("Lowercase filter explain plan")
    addressRead.filter("lower(externalId)='326686'").select("id", "externalId").explain(true)

    // Print out the resulting dataframe
    println("Filter with projection (pushdown) result:")
    dfWithFilterAndProjection.show()

    /*
    since the filter was on a non hashed column it had to do a scan on each partition in parallel
    So the number of partitions is 10 for the dataframe
    if the filter was on a hashed column the number of partitions would be 1 as it would partition prune the other partitions
     */
    println("number of partitions for filter and projection:"+dfWithFilterAndProjection.rdd.getNumPartitions)


    // when using one of the hashed columns for a filter it can partition prune the result
    println("number of partitions for hashed filter and projection:"+addressRead.filter("id=41206262").select("id", "externalId").rdd.getNumPartitions)


    //explore the different methods available on the kudu context below and also different filtering and projections
    // see what filters are supported and not supported by the kudu spark datasource
    // Be sure to check out the copied unit tests in DefaultSourceTest and KuduContextTest for a wide range of filtering examples


    val tenRows = dfWithFilterAndProjection.limit(10);
    //more advanced features are available through the kudu context.. This is because spark only supports so much through the datasource api
    //delete example
    //kuduContext.deleteRows(tenRows, "address")

    //    kuduContext.insertRows(tenRows, "address")
    // at times you may not know if you want to insert or update... so kudu has server side upsert to avoid doing an expensive read and write operation
    //    kuduContext.upsertRows(tenRows, "address")


  }
}
