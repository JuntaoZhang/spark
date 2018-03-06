package org.apache.spark.mytest

/**
 * @author juntao zhang
 */
package basetest {

  import scala.collection.mutable
  import scala.ref.WeakReference

  object WeakReferenceTest {

    class Person {
      val age = 31
    }

    def main(args: Array[String]): Unit = {
      var person = new Person
      val originals = mutable.Map[Long, WeakReference[Person]]()
      originals(1) = WeakReference[Person](person)
      println(originals(1).get)
      // 释放强引用,手动触发gc
      person = null;System.gc()
      println(originals(1).get)
    }
  }

}
