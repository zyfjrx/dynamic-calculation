package com.byt.tagcalculate.sink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/7/31 10:56
 **/
public class MqttSink extends RichSinkFunction<String> {
    private transient MqttClient mqttClient;
    private String broker;
    private String topic;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        mqttClient = new MqttClient(parameterTool.get("mqtt.hostUrl"), MqttClient.generateClientId());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        mqttClient.connect(options);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        MqttMessage message = new MqttMessage(value.getBytes());
        mqttClient.publish(parameterTool.get("mqtt.sink.topic"), message);
    }

    @Override
    public void close() throws Exception {
        if (mqttClient != null) {
            mqttClient.disconnect();
            mqttClient.close();
        }
        super.close();
    }
}
