import java.util.BitSet;

/**
 * @author juntao zhang
 */
public class MyTest {

  public static void main(String[] args) {
    BitSet bs = new BitSet();
    bs.set(2);
    bs.set(5);
    System.out.println(bs.get(1));
  }
}
