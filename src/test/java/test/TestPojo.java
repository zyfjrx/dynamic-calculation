package test;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/19 14:13
 **/
public class TestPojo {
    public String word;
    public Integer value;
    public long timestamp;
    public long winSize;
    public long winSlide;
    public TestPojo() {
    }

    public TestPojo(String word,Integer value, long timestamp, long winSize, long winSlide) {
        this.word = word;
        this.value = value;
        this.timestamp = timestamp;
        this.winSize = winSize;
        this.winSlide = winSlide;
    }

    public static TestPojo of(String word,Integer value, long timestamp, long winSize, long winSlide){
        return new TestPojo(word,value,timestamp,winSize,winSlide);
    }
}
