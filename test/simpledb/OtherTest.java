package simpledb;

import org.junit.Test;

public class OtherTest {
    @Test
    public void test() {
        String min_s = "";
        String max_s = "zzzz";
        String s = max_s;

        int i;
        int v = 0;
        for (i = 3; i >= 0; i--) {
            if (s.length() > 3 - i) {
                int ci = s.charAt(3 - i);
                v += (ci) << (i * 8);
            }
        }

        System.out.println(v);
    }

}

