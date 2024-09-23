package test;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/10/27 10:41
 **/
public class Test1 {
    public static void main(String[] args) {

        String regex = "[/:.]";
        String replacement = "_";

        String result = "ooomvvvvvvv".replaceAll(regex, replacement);
        System.out.println(result);
    }
}
