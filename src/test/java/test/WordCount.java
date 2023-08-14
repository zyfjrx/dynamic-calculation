package test;


import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool defaultPropertiesFile = ParameterTool.fromPropertiesFile(
                WordCount.class.getClassLoader().getResourceAsStream("application.properties")
        );

        System.out.println(defaultPropertiesFile.get("mysql.host"));

    }
}
