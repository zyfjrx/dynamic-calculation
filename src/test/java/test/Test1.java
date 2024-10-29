package test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/10/27 10:41
 **/
public class Test1 {
    public static void main(String[] args) {


        System.out.println(UUID.nameUUIDFromBytes("ods".getBytes(StandardCharsets.UTF_8)).toString());
        System.out.println(UUID.randomUUID().toString());
    }
}
