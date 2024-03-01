package test;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/10/27 10:41
 **/
public class Test1 {
    public static void main(String[] args) {

        String string = String.valueOf(1709190025006L);
        String substring = string.substring(0, string.length() - 4);
        System.out.println(substring);
    }
}
