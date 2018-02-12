// scalastyle:off
package org.apache.spark.mytest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.CartesianRDD

/**
 * @author juntao zhang
 */
object CartesianRDDTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Join")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val ones = sc.makeRDD(1 to 3, 2).map(x => x)
    val twos = sc.makeRDD(Seq('a','b','c','d','e'), 3).map(x => x)
    val cartesian = new CartesianRDD(sc, ones, twos)
    println(cartesian.collect())

    //    val player = sc.parallelize(List(("ACMILAN", "KAKA"), ("ACMILAN", "BT"), ("GUANGZHOU", "ZHENGZHI")))
    //    val team = sc.parallelize(List(("ACMILAN", 5), ("GUANGZHOU", 3)))
    //    val res =  player.join(team).collect()
    //    println(res.mkString(","))

    sc.stop()
  }
}
