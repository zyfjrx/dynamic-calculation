package com.byt.tagcalculate.calculate;

import java.util.ArrayList;
import java.util.List;

/**
 * @title: 算子链路
 * @author: zhangyifan
 * @date: 2022/8/30 21:43
 */
public class TransformChain {
    private List<Transform> transformers = new ArrayList<>();
    private int index = 0;

    public TransformChain add(Transform t){
        transformers.add(t);
        return this;
    }

    public void doTransform(TStream tStream){
        if (index == transformers.size()) return;
        Transform t = transformers.get(index);
        index += 1;
        t.doTransForm(tStream,this);
    }

}
