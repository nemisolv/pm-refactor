package com.viettel;

import com.viettel.util.Util;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties
public class MainApplication {
    public static void main(String[] args) {
        ConfigurableApplicationContext appContext = SpringApplication.run(MainApplication.class, args);
//        ScheduleService scheduleService = appContext.getBean(ScheduleService.class);
        Util util = appContext.getBean(Util.class);
//        try {
//            scheduleService.runOne();
//        }catch( ScheduleException e) {
//            e.printStackTrace();
//        }
    }

}
