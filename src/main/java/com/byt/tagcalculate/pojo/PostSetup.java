package com.byt.tagcalculate.pojo;

import java.util.HashSet;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/6/27 16:21
 */
public class PostSetup {
    private String postName;
    private HashSet<String> postKeys;
    private String postUrl;

    public String getPostName() {
        return postName;
    }

    public void setPostName(String postName) {
        this.postName = postName;
    }

    public HashSet<String> getPostKeys() {
        return postKeys;
    }

    public void setPostKeys(HashSet<String> postKeys) {
        this.postKeys = postKeys;
    }

    public String getPostUrl() {
        return postUrl;
    }

    public void setPostUrl(String postUrl) {
        this.postUrl = postUrl;
    }

    @Override
    public String toString() {
        return "PostSetup{" +
                "postName='" + postName + '\'' +
                ", postKeys=" + postKeys +
                ", postUrl='" + postUrl + '\'' +
                '}';
    }
}
