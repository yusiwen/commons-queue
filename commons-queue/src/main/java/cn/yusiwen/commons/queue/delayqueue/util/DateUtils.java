package cn.yusiwen.commons.queue.delayqueue.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Utilities for dates
 *
 * @author Siwen Yu
 * @since 1.0.1
 */
public class DateUtils {

    /**
     * DateUtils
     */
    private DateUtils() {

    }

    /**
     * Compare a {@code LocalDateTime} with current time
     *
     * @param diff The {@code LocalDateTime} to be compared
     * @return Difference
     */
    public static Duration getExpiration(LocalDateTime diff) {
        return Duration.ofSeconds(ChronoUnit.SECONDS.between(LocalDateTime.now(), diff));
    }

    /**
     * Compare two {@code LocalDateTime}
     *
     * @param first The first {@code LocalDateTime} to be compared
     * @param second The second {@code LocalDateTime} to be compared
     * @return Difference between {@code first} and {@code second}
     */
    public static Duration getExpiration(LocalDateTime first, LocalDateTime second) {
        return Duration.ofSeconds(ChronoUnit.SECONDS.between(first, second));
    }

    /**
     * Convert a {@code Date} to {@code LocalDateTime}
     *
     * @param dateToConvert Date to be converted
     * @return Converted {@code LocalDateTime}
     */
    public static LocalDateTime convertDateToLocalDateTime(Date dateToConvert) {
        return Instant.ofEpochMilli(dateToConvert.getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    /**
     * Convert a given cron string to {@code Date}
     *
     * @param cron Cron string
     * @return {@code Date} of that cron string
     */
    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "LEST_LOST_EXCEPTION_STACK_TRACE"})
    public static LocalDateTime convertCron(String cron) {
        String dateFormat = "ss mm HH dd MM ? yyyy";
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        Date date = null;
        try {
            date = sdf.parse(cron);
        } catch (ParseException e) {
            throw new IllegalArgumentException("invalid cron string: " + cron);
        }
        return convertDateToLocalDateTime(date);
    }
}
