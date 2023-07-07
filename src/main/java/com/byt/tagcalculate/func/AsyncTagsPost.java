package com.byt.tagcalculate.func;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class AsyncTagsPost extends RichAsyncFunction<Tuple2<String, String>, HashMap<String, String>> {
    private transient CloseableHttpAsyncClient httpclient;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RequestConfig re = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();
        httpclient = HttpAsyncClients.custom()
                .setMaxConnTotal(20)
                .setDefaultRequestConfig(re)
                .build();
        httpclient.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        httpclient.close();
    }

    @Override
    public void timeout(Tuple2<String, String> input, ResultFuture<HashMap<String, String>> resultFuture) throws Exception {
        System.out.println("----------api time out----------");
    }

    @Override
    public void asyncInvoke(Tuple2<String, String> input, ResultFuture<HashMap<String, String>> resultFuture) throws Exception {
        String url = input.f0;
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader(HTTP.CONTENT_TYPE, "application/json");
        String jsonstr = input.f1;
        StringEntity se = new StringEntity(jsonstr);
        se.setContentType("text/json");
        se.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
        httpPost.setEntity(se);
        Future<HttpResponse> future = httpclient.execute(httpPost, null);
        CompletableFuture.supplyAsync(() -> {
            try {
                HttpResponse response = future.get();
                String result = EntityUtils.toString(response.getEntity());
                HashMap<String, String> res = JSON.parseObject(result, HashMap.class);
                return res;
            } catch (Exception e) {
                return null;
            }
        }).thenAccept((HashMap<String, String> res) -> {
            resultFuture.complete(Collections.singleton(res));
        });

    }
}
