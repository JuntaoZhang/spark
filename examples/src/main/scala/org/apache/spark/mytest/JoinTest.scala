package org.apache.spark.mytest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author juntao zhang
 */
object JoinTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Join")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val player = sc.parallelize(List(("a", "A1"), ("a", "A2"), ("b", "B")))
    /*.filter(tuple=>tuple._1 != "test")*/
    val team = sc.parallelize(List(("a", 1), ("b", 2)))
    /*.distinct()*/
    val join = player.join(team)
    val res = join.collect()

    //    val res = sc.parallelize(List(("a", "A1"), ("a", "A2"), ("b", "B"))).collect()


    println(res.mkString(","))

    sc.stop()
  }
}

object FirstTest {

  class Person {}

  class Man extends FirstTest.Person {}

  class Test {
    def run(person: FirstTest.Person): Unit = {
      System.out.println("this is person fun")
    }

    def run(man: FirstTest.Man): Unit = {
      System.out.println("this is man fun")
    }
  }

  def main(args: Array[String]): Unit = {
    val person = new FirstTest.Man
    val test = new FirstTest.Test
    test.run(person)
    test.run(person.asInstanceOf[FirstTest.Man])
  }
}

object AccumulatorTest {
  val conf = new SparkConf().setAppName("Spark Join")
  conf.setMaster("local[*]")
  val sc = new SparkContext(conf)

  val accum1 = sc.accumulator(2000, "total even")
  val accum2 = sc.accumulator(1000, "total odd")
  val data = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2).map(x => {
    if (x % 2 == 0) accum1 += x
    else accum2 += x
    x
  })
  data.cache().distinct().count()
  //  data.foreach(println)
  //  data.foreach(accum += _)
  println(accum1.value)
  println(accum2.value)

  sc.stop()
}

object ObjTest {

  class Person {

    def read() = {
      println("read age")
      31
    }

    val age = read()
  }

  def main(args: Array[String]): Unit = {
    println(new Person)
  }
}
