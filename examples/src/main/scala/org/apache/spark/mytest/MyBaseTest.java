package org.apache.spark.mytest;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

/**
 * @author juntao zhang
 */
public class MyBaseTest {
  static class Person{}
  public static void main(String[] args) {
    Map<Long,WeakReference> map = new HashMap<>();
    map.put(new Long(1),new WeakReference<>(new Person()));
    System.out.println(map.get(new Long(1)).get());
    System.gc();
    System.out.println(map.get(new Long(1)).get());

  }
}
