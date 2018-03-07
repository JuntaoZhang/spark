package org.apache.spark.mytest

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author juntao zhang
 */
object SqlTest {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Join")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
//    val people = sc.textFile("examples/src/main/resources/people.txt")
    val people = sc.textFile("/tmp/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
      .toDF()
    people.registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    sc.stop()
  }
}
