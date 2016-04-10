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
package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object SimpleJsonSQLTest extends App {
  val sparkConf = new SparkConf().setAppName("json")
  sparkConf.setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val sqlContext = new SQLContext(sc)

  val personDF = sqlContext.read.json("examples/src/main/resources/people.json")
  personDF.show()
//  personDF.registerTempTable("p")
//  sqlContext.sql("SELECT * FROM p").show()
}

object SimpleDFJsonTest extends App {
  val sparkConf = new SparkConf().setAppName("json")
  sparkConf.setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val df = sqlContext.read.json("examples/src/main/resources/persons.json")
  df.select(df("age"), df("department"))
    .filter(df("age") < 31)
    .groupBy("department")
    .count()
    .show()
}

object JsonDemo extends App {
  val sparkConf = new SparkConf().setAppName("json")
  sparkConf.setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  val jsonData = Seq("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}")
  val rdd = sc.parallelize(jsonData)

  val df = sqlContext.read.json(rdd)
  df.show()
  df.printSchema()
}

object JsonSQLDemo extends App {
  val sparkConf = new SparkConf().setAppName("json")
  sparkConf.setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val sqlContext = new SQLContext(sc)

  val personDF = sqlContext.read.json("local/json/people2.json")

  personDF.registerTempTable("p")
  sqlContext.sql("SELECT address.city,count(address.city) FROM p group by address.city").show()

}

object JsonHttpSessionSQL extends App {
  val sparkConf = new SparkConf().setAppName("json")
  sparkConf.setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val personDF = sqlContext.read.json("local/json/http_session.json")
  personDF.registerTempTable("p")
  sqlContext.sql(
    "SELECT destAddrCity.City,count(destAddrCity.City) FROM p group by destAddrCity.City").show()
}
