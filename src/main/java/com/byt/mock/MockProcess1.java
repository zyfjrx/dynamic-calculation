package com.byt.mock;

import com.byt.pojo.TopicData;
import com.byt.protos.TagKafkaProtos;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Random;

/**
 * @title: 模拟数据发送
 * @author: zhangyifan
 * @date: 2022/10/14 08:59
 */
public class MockProcess1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<TopicData> result = env
                .addSource(new RichSourceFunction<MockBean>() {
                    // 控制发送的个数和cancel程序
                    Integer times = 5000000;
                    private Random random = new Random();
                    // 待发送标签
                    private String[] tagNames = {"aj-aa"};
                    private Integer[] values = {1, 2, 5, 8, 10, 11, 13, 14, 17, 18, 20, 21, 24, 25, 26, 27, 28, 31, 32, 33, 34, 35, 38, 39, 41, 42, 43, 45, 46, 48,
                            49, 50, 51, 53, 56, 57, 58, 59, 60, 61, 62, 63, 65, 69, 72, 78, 79, 81, 82, 83, 87, 88, 89, 90, 91, 93, 94, 95, 96,
                            97, 99, 101, 102, 103, 105, 106, 108, 111, 112, 113, 115, 116, 117, 121, 122, 123, 124, 125, 126, 127, 129, 130,
                            131, 132, 135, 136, 138, 140, 141, 142, 145, 146, 147, 148, 151, 152, 154, 157, 159, 161, 162, 163, 167, 168, 175,
                            176, 177, 178, 179, 180, 183, 184, 185, 187, 188, 190, 192, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204,
                            206, 208, 210, 212, 213, 216, 218, 219, 221, 222, 223, 224, 225, 226, 233, 236, 237, 241, 243, 245, 246, 247, 248,
                            251, 252, 253, 254, 255, 258, 264, 267, 268, 270, 271, 273, 274, 275, 277, 278, 279, 280, 286, 288, 290, 291, 292,
                            293, 296, 297, 298, 299, 302, 303, 308, 310, 312, 314, 315, 316, 317, 318, 319, 321, 322, 323, 324, 327, 329, 334,
                            335, 336, 338, 340, 341, 342, 343, 344, 347, 348, 349, 351, 352, 353, 355, 357, 358, 359, 360, 362, 363, 364, 366,
                            368, 370, 371, 374, 375, 378, 379, 380, 381, 382, 384, 386, 387, 390, 391, 393, 394, 395, 396, 398, 399, 400, 402,
                            403, 404, 405, 407, 408, 409, 411, 414, 415, 416, 417, 418, 420, 421, 422, 424, 425, 427, 428, 429, 431, 432, 434,
                            435, 436, 438, 439, 440, 443, 444, 445, 447, 448, 451, 452, 454, 456, 457, 458, 459, 462, 463, 464, 467, 468, 469,
                            470, 471, 472, 474, 476, 477, 478, 479, 482, 483, 485, 486, 487, 488, 491, 492, 495, 497, 499, 500, 501, 505, 507,
                            508, 509, 510, 514, 516, 517, 518, 519, 520, 521, 522, 523, 525, 529, 530, 532, 533, 534, 535, 536, 542, 545, 546,
                            547, 551, 552, 553, 555, 557, 559, 560, 561, 563, 566, 567, 569, 571, 573, 574, 575, 576, 577, 578, 579, 580, 584,
                            585, 586, 587, 588, 590, 591, 593, 594, 595, 596, 601, 602, 603, 605, 607, 608, 610, 612, 613, 614, 615, 616, 618,
                            619, 621, 622, 623, 624, 626, 627, 628, 630, 631, 633, 634, 635, 637, 638, 640, 642, 643, 644, 645, 646, 647, 649,
                            650, 652, 654, 655, 656, 658, 659, 664, 665, 666, 667, 669, 670, 672, 675, 676, 678, 682, 683, 684, 685, 686, 687,
                            690, 691, 694, 695, 696, 697, 698, 699, 701, 702, 704, 705, 707, 708, 710, 711, 713, 715, 717, 718, 719, 720, 721,
                            722, 723, 724, 725, 726, 728, 730, 731, 732, 736, 737, 738, 739, 743, 744, 748, 749, 750, 751, 752, 753, 754, 755,
                            756, 757, 759, 760, 763, 764, 766, 767, 771, 775, 776, 777, 779, 782, 783, 784, 785, 786, 789, 790, 791, 792, 793,
                            794, 795, 796, 797, 798};

                    @Override
                    public void run(SourceContext<MockBean> ctx) throws Exception {
                        while (times > 0) {
                            MockBean mockBean = new MockBean(
                                    tagNames[random.nextInt(tagNames.length)],
                                    values[random.nextInt(values.length)],
                                    //random.nextInt(1000),
                                    //999,
                                    Calendar.getInstance().getTimeInMillis()
                            );
                            ctx.collect(mockBean);
                            System.out.println("发送数据：" + mockBean + ",发送时间：" + new Timestamp(mockBean.getTs()));
                            times--;
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        times = -1;
                    }
                })
                .map(new RichMapFunction<MockBean, Tuple3<String, String, String>>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                    }

                    @Override
                    public Tuple3<String, String, String> map(MockBean value) throws Exception {
                        return Tuple3.of(value.getTagName(), value.getValue().toString(), sdf.format(value.getTs()));
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new ProcessAllWindowFunction<Tuple3<String, String, String>, TopicData, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple3<String, String, String>, TopicData, TimeWindow>.Context context, Iterable<Tuple3<String, String, String>> elements, Collector<TopicData> out) throws Exception {
                        ArrayList<Value> lvOutBuild = new ArrayList<>();
                        Iterator<Tuple3<String, String, String>> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            Tuple3<String, String, String> tag = iterator.next();
                            TagKafkaProtos.TagKafkaInfo newTag = TagKafkaProtos.TagKafkaInfo.newBuilder()
                                    .setName(tag.f0)
                                    .setValue(tag.f1)
                                    .setTime(tag.f2)
                                    .build();
                            lvOutBuild.add(Value.newBuilder().mergeFrom(newTag.toByteArray()).build());

                        }
                        ListValue lvOut = ListValue.newBuilder()
                                .addAllValues(lvOutBuild)
                                .build();

                        TopicData topicData = new TopicData();
                        // 模拟数据发送的topic
                        topicData.setTopic("opc-data");
                        topicData.setData(lvOut.toByteArray());

                        out.collect(topicData);
                    }
                });
        result
                .addSink(MyKafkaUtilMock.getProducerWithTopicData());


        env.execute();
    }
}
