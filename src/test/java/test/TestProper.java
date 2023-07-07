package test;

import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.common.utils.ConfigManager;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/3/1 13:10
 **/
public class TestProper {
    public static void main(String[] args) {
        System.out.println(ConfigManager.getProperty(PropertiesConstants.KAFKA_ODS_TOPIC));
    }
}
