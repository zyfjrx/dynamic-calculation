package com.byt.tagcalculate.mock;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/10/14 09:07
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MockBean2 {
    private String tagName;
    private String value;
    private Long ts;
}
