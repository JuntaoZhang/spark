package org.apache.spark.mytest

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers

/**
 * @author juntao zhang
 */
object ParseTest extends RegexParsers {
  val number: Regex = "[0-9]+".r
  // 表达式
  def expr(): Parser[Any] = term ~ (("+" | "-") ~ expr).?
  //
  def term(): Parser[Any] = factor ~ ("*" ~ factor ).*
  // 因子
  def factor: Parser[Any] = "(" ~ expr ~ ")" | number
  def main(args: Array[String]) {
    val input = "1+(4-2*4)*2+3"
    val res = ParseTest.parseAll(ParseTest.expr(),input)
    println(res)
  }
}

object SqlTest {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Join")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits.rddToDataFrameHolder
//    val people = sc.textFile("examples/src/main/resources/people.txt")
    val people = sc.textFile("/tmp/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
      .toDF()
    people.registerTempTable("people")
    val teenagers1 = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
//    teenagers.explain(true)
    teenagers1.collect()
//    teenagers1.show()

    sc.stop()
  }
}
