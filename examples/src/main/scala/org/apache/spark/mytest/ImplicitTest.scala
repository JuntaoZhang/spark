// scalastyle:off
package org.apache.spark.mytest

/**
  * @author juntao zhang
  */
object ImplicitTest {

  class MyRDD {
    def map(): Unit = {
      println("this map")
    }

    override def toString = s"MyRDD()"
  }

  object MyRDD {
    implicit def rddToPairRDDFunctions(rdd: MyRDD): MyPairRDDFunctions = {
      new MyPairRDDFunctions(rdd)
    }
  }

  class MyShuffledRDD(prev: MyRDD) extends MyRDD {
    override def toString = s"MyShuffledRDD()"
  }

  class MyPairRDDFunctions(self: MyRDD) {
    def reduceByKey(): MyShuffledRDD = {
      println("this reduceByKey")
      new MyShuffledRDD(self)
    }
  }

  def main(args: Array[String]): Unit = {
    println(new MyRDD().reduceByKey())
  }
}

// scalastyle:on
