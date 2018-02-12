// scalastyle:off
package org.apache.spark.mytest

import org.apache.spark.rdd.CartesianRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author juntao zhang
  */
object JoinTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Join")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

//    val player = sc.parallelize(List(("a", "A1"), ("a", "A2"), ("b", "B"))).filter(tuple=>tuple._1 != "test")
//    val team = sc.parallelize(List(("a", 1), ("b", 2))).distinct()
//    val join =  player.join(team)
//    val res = join.collect()

    val res = sc.parallelize(List(("a", "A1"), ("a", "A2"), ("b", "B"))).collect()


    println(res.mkString(","))

    sc.stop()
  }
}
// scalastyle:on
