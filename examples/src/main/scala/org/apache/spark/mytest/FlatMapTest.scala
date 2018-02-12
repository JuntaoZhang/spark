// scalastyle:off
package org.apache.spark.mytest
import java.util.{HashMap => JHashMap}
import scala.collection.JavaConverters._

/**
  * @author juntao zhang
  */
object FlatMapTest extends App{
  val res = Seq(
    "Return a new RDD by applying",
    "a function to all elements of this RDD"
  ).flatMap(line=>line.split(" "))
  println(res)


  val map = new JHashMap[Int, List[Int]]{{
    put(1,List(1,2,3))
    put(2,List(1,2,3))
    put(3,List(4,2,3))
  }}.asScala
  map.foreach(println)
  val res2 = map.iterator.flatMap(t => t._2.iterator.map((t._1, _)))
  res2.toSeq.foreach(println)
}
// scalastyle:on