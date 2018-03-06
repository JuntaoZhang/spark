package org.apache.spark.mytest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author juntao zhang
 */
object ClosureCleanerTest {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("Spark Join")
      conf.setMaster("local[*]")
      val sc = new SparkContext(conf)

//      def someValue = 1
//      def scope(name: String)(body: => Unit) = body
//      def someMethod(): Unit = scope("one") {
//        def x = someValue
//        def y = 2
//        scope("two") { println(y + 1) }
//      }
//
//      val f = sc.clean(scope("one") {
//        def x = someValue
//        def y = 2
//        scope("two") { println(y + 1) }
//      })

//      println(f)

      sc.stop()
    }
  }
}
