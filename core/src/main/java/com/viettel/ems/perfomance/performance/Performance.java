package com.viettel.ems.perfomance.performance;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.util.StopWatch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

@Data
@Slf4j
public class Performance {
    private final StringBuilder sb = new StringBuilder();
    private final StopWatch total = new StopWatch();
    private final String name;
    private static String IPAddress;
    private static String serviceCode;
    private static String inputString;

    public Performance(String name) {
        this.name = name;
        total.start();
    }

    public static Performance of (String name, String code, String input)  {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            IPAddress = inetAddress.getHostAddress();
            serviceCode = code;
            inputString = input;
        }catch (UnknownHostException e) {
            log.error("Failed to get local host address", e);
        }
        return new Performance(name);
    }

    public static Duration getDuration(StopWatch stopWatch) {
        var infos = stopWatch.getTaskInfo();
        if (infos.length == 0) return Duration.ZERO;
        return Duration.ofNanos(infos[infos.length - 1].getTimeNanos());
    }

    public void log(String output) {
        synchronized (sb) {
            total.stop();
            log.info("{} {} {} {} {} {}: {}\n{}", "PM", serviceCode, name, IPAddress, inputString, output, formatDuration(getDuration(total)), sb.toString());
        }
    }
    private String formatDuration(Duration duration) {
        long millis = duration.toMillis();
        long micros = duration.toNanos() / 1000;
        if (millis >= 1000) {
            // Nếu >= 1 giây thì in ra giây với phần thập phân
            double seconds = millis / 1000.0;
            return String.format("%.3f s", seconds); // vd: 1.234 s
        }else if (millis > 0) {
            return millis + " ms";
        } else {
            return micros + " µs";
        }
    }


    public <T> T trace(Callable<T> runnable, String format, Object... logObjects) {
        var stackTrace = Thread.currentThread().getStackTrace();

        var stopWatch = new StopWatch();
        stopWatch.start();
        try {
            return runnable.call();
        } finally {
            stopWatch.stop();
            parseStackTrace(stackTrace, format, logObjects, getDuration(stopWatch));
        }
    }

    public void trace(Runnable runnable, String format, Object... logObjects) {
        var stackTrace = Thread.currentThread().getStackTrace();

        var stopWatch = new StopWatch();
        stopWatch.start();
        try {
             runnable.run();
        } finally {
            stopWatch.stop();
            parseStackTrace(stackTrace, format, logObjects, getDuration(stopWatch));
        }
    }

    public void parseStackTrace(
            StackTraceElement[] stackTrace, String format, Object[] logObjects, Object output, Duration duration
    ) {
        var caller = stackTrace[2];
        var msg = MessageFormatter.arrayFormat(format, logObjects).getMessage();
        synchronized (sb) {
            sb.append("\n -> ")
                    .append(caller.getClassName())
                    .append(".")
                    .append(caller.getMethodName())
                    .append(": ")
                    .append(msg)
                    .append(" -> Output: ")
                    .append(output)
                    .append("; Duration: ")
                    .append(duration);
        }
    }


  public void parseStackTrace(
        StackTraceElement[] stackTrace, String format, Object[] logObjects,  Duration duration
    ) {
        var caller = stackTrace[2];
        var msg = MessageFormatter.arrayFormat(format, logObjects).getMessage();
        synchronized (sb) {
            sb.append("\n -> ")
            .append(caller.getClassName())
            .append(".")
            .append(caller.getMethodName())
            .append(": ")
            .append(msg)
            .append(" ->  ")
            .append("; Duration: ")
            .append(duration);
        }
    }

    public interface Callable<T> {
        T call();
    }
}
