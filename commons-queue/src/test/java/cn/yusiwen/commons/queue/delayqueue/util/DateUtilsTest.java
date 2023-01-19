package cn.yusiwen.commons.queue.delayqueue.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

import org.junit.jupiter.api.Test;

class DateUtilsTest {

    static final String FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Test
    void testGetCronToDate() {
        DateTimeFormatter df = DateTimeFormatter.ofPattern(FORMAT);
        LocalDateTime date = DateUtils.convertCron("00 45 16 19 01 ? 2023");
        String dateStr = df.format(date);

        DateFormat dateFormat = new SimpleDateFormat(FORMAT);
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.JANUARY, 19, 16, 45, 0);
        Date actualDate = calendar.getTime();
        String actualDateStr = dateFormat.format(actualDate);

        assertThat(actualDateStr, equalTo(dateStr));
    }

    @Test
    void testGetExpiration() {
        LocalDateTime origin = LocalDateTime.now();
        LocalDateTime diff = origin.plusSeconds(100);
        Duration duration = DateUtils.getExpiration(origin, diff);

        assertThat(duration.getSeconds(), equalTo(100L));
    }

    @Test
    void shouldGetNegativeNumber() {
        LocalDateTime origin = LocalDateTime.now();
        LocalDateTime diff = origin.plusSeconds(-100);
        Duration duration = DateUtils.getExpiration(origin, diff);
        System.out.println(duration.getSeconds());

        assertThat(duration.getSeconds(), equalTo(-100L));
    }
}
