package com.viettel.ems.perfomance.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;


@Configuration
public class ThreadPoolConfig {

    @Bean
    public ThreadPoolTaskScheduler threadPoolTaskSchedulerParse() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(20);
        scheduler.setThreadNamePrefix("threadPoolTaskSchedulerParse");
        return scheduler;
    }

    @Bean
    public ThreadPoolTaskScheduler threadPoolTaskSchedulerDataLake() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(20);
        scheduler.setThreadNamePrefix("threadPoolTaskSchedulerDataLake");
        return scheduler;
    }

    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(10);
        scheduler.setThreadNamePrefix("SystemScheduler-");
        scheduler.setAwaitTerminationSeconds(30);
        return scheduler;
    }


}