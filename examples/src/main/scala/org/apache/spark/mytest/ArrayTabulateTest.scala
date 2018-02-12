// scalastyle:off
package org.apache.spark.mytest

/**
  * @author juntao zhang
  */
object ArrayTabulateTest extends App {

  case class Key(idx: Int)

  println(Array.tabulate(10)((i) => Key(i)).mkString(","))
}

// scalastyle:on