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

package org.apache.spark.examples

import org.apache.spark._

/** wordcount */
object WordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: Wordcount <file>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("Wordcount")
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.shuffle.manager", "hash")
    val sc = new SparkContext(sparkConf)

    val res = sc
      .textFile(args(0))
      .flatMap(line => line.split(" "))
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .collect()

    res.foreach(tuple => println(tuple._1 + " => " + tuple._2))

    sc.stop()
  }
}

object MyTest extends Logging {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "test")
    var results = sc.parallelize(List(1, 2, 3, 3, 4, 5, 13, 4, 15))
      .map(x => {
        logDebug(s"map1 $x => ${(x, 1)}")
        (x, 1)
      })
      .map(x => {
        logDebug(s"map2 $x => ${(x._1, x._2, 2)}")
        (x._1, (x._1, x._2, 2))
      })
      .groupByKey(3)
      .collect()
      .foreach(tuple => println(tuple._1 + " : " + tuple._2))
  }
}

object Top10 {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: Top10 <file>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("Top10")
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile(args(0))
    file.flatMap(line => line.split(" "))
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .sortBy(p => p._2)
      .take(10)
      .foreach(tuple => println(tuple._1 + " : " + tuple._2))
    sc.stop()
  }
}
