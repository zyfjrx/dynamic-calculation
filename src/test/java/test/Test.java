package test;

import lombok.EqualsAndHashCode;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/10/11 14:12
 */
@EqualsAndHashCode
public class Test {
    public static void main(String[] args) throws Exception {
        long lastStart = 5;
        for (long start = lastStart; start > 13 - 10; start -= 5) {
            System.out.println(start);
            System.out.println(start+5);
        }
    }
}
