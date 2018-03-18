package org.apache.spark.examples.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * @author juntao zhang
 */
object SQLTest {
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Spark Join").getOrCreate()
    import spark.implicits._
    //    val people = sc.textFile("examples/src/main/resources/people.txt")
    val people = spark.sparkContext.textFile("/tmp/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
      .toDF()
    people.createOrReplaceTempView("people")
    val teenagers1 = spark.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
    //    teenagers.explain(true)
    teenagers1.collect()
//        teenagers1.show()

    spark.stop()
  }
}
