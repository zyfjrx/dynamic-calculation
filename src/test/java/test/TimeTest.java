package test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/10/18 9:28
 **/
public class TimeTest {
    public static void main(String[] args) {
        //设置时区
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+8"));
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        long zero = calendar.getTimeInMillis();
        System.out.println(calendar.getTimeInMillis());

        System.out.println(LocalDateTime.ofInstant(Instant.ofEpochMilli(zero), ZoneOffset.ofHours(8)));

/*        System.out.println(LocalDateTime.now());
        LocalDateTime dateTime = LocalDateTime.now();
        System.out.println(dateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli());
        System.out.println(LocalDateTime.ofInstant(Instant.ofEpochMilli(1697587200000L), ZoneOffset.ofHours(0)));*/
    }
}
