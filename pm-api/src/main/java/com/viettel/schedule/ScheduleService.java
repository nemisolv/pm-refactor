package com.viettel.schedule;

import com.viettel.config.*;
import com.viettel.dal.InputStatisticData;
import com.viettel.model.event.LeaderChangedEvent;
import com.viettel.repository.CommonRepository;
import com.viettel.repository.CounterKpiRepository;
import com.viettel.troubleshoot.DatasourceVerifier;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


@Slf4j
@Component
public class ScheduleService {

    @Autowired
    private CommonRepository commonRepository;

    @Autowired
    private ScheduleTask scheduleTask;

    @Autowired
    private LeaderElectionService leaderElectionService;

    @Autowired
    @Qualifier("datasourceVerifier")
    private DatasourceVerifier datasourceVerifier;

    @Autowired
    private ConfigManager configManager;

    @Autowired
    private RoutingContextExecutor routingContextExecutor;

    @EventListener
    @Async
    public void changeLeader(LeaderChangedEvent event) throws SchedulerException {
        Scheduler scheduler = new StdSchedulerFactory().getScheduler();
        if (scheduler != null) {
            scheduler.clear();
            scheduler.shutdown(true);
        }

        if (leaderElectionService.isLeader()) {
            log.info("Leader changed!, reload all schedule");
            for (SystemType systemType : SystemType.values()) {
                if (configManager.isDeployed(systemType)) {
                    routingContextExecutor.runWith(systemType, "PRIMARY", () -> {
                        try {
                            commonRepository.updateScheduleJobName();
                            runOne();
                        } catch (SchedulerException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            }
        }
    }

    // @Scheduled(cron = "${cron-schedule}")
    // public void runRunOne() throws SchedulerException {
    //     runOne();
    // }

    @SystemScheduled(key = "runOne", datasource = "PRIMARY")
    public void runOne() throws SchedulerException {
        log.info("Schedule Print Log");
        datasourceVerifier.verifyConnection();

        if (!leaderElectionService.isLeader()) {
            log.info("instance is not leader");
            return;
        }

        // Get all current schedule
        List<ScheduleQuery> ls = commonRepository.getScheduleIsActive();
        List<ScheduleQuery> currentJob = new ArrayList<>();

        for (ScheduleQuery l : ls) {
            if (l.getJobName() == null) {
                continue;
            }

            String cronString = l.getCronString().toCronString();
            if (CronExpression.isValidExpression(cronString)) {
                String jobName = startNewJobSchedule(l.getName(), l.getInputStatisticData().getSystemType(), cronString, l, scheduleTask);
                if (commonRepository.updateScheduleJobName(jobName, l.getId()) <= 0) {
                    stopSchedule(jobName);
                }
                log.info("start cron {} {}", l.getName());
                l.setJobName(jobName);
                currentJob.add(l);
            } else {
                log.error("invalid Cron String for {}", l.getName());
            }
        }

        checkJobRunning(currentJob);
    }

    public void checkJobRunning(List<ScheduleQuery> currentJobDB) throws SchedulerException {
        Scheduler scheduler = new StdSchedulerFactory().getScheduler();

        for (String groupName : scheduler.getJobGroupNames()) {
            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                String jobName = jobKey.getName();
                String jobGroup = jobKey.getGroup();

                // get job's trigger
                CronTrigger trigger = (CronTrigger) scheduler.getTriggersOfJob(jobKey).get(0);
                if (currentJobDB.size() > 0) {
                    ScheduleQuery dbSchedule = null;
                    try {
                        dbSchedule = currentJobDB.stream()
                                .filter(item -> item.getJobName().equals(jobName))
                                .findFirst()
                                .orElse(null);
                    } catch (Exception ignored) {
                    }

                    if (dbSchedule != null) {
                        String dbCron = dbSchedule.getCronString().toCronString();
                        if (!trigger.getCronExpression().equals(dbCron)) {
                            CronTrigger newTrigger = TriggerBuilder.newTrigger()
                                    .withIdentity("cron_" + jobName, jobGroup)
                                    .withSchedule(CronScheduleBuilder.cronSchedule(dbCron))
                                    .forJob(jobName, jobGroup)
                                    .startNow()
                                    .build();

                            scheduler.rescheduleJob(trigger.getKey(), newTrigger);
                            log.info("[Schedule Report] Update crontab for job: {}", jobName);
                            trigger = (CronTrigger) scheduler.getTriggersOfJob(jobKey).get(0);
                        }
                    } else {
                        stopSchedule(jobName);
                        continue;
                    }
                }

                Date nextFireTime = trigger.getNextFireTime();
                log.info("[jobName] : {} [groupName] : {} - {}", jobName, jobGroup, nextFireTime);
            }
        }
    }

    public void stopSchedule(String jobName) throws SchedulerException {
        Scheduler scheduler = new StdSchedulerFactory().getScheduler();
        for (String groupName : scheduler.getJobGroupNames()) {
            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                if (jobKey.getName().equals(jobName)) {
                    log.info("[Schedule Report] Delete job {} {}", jobName, scheduler.deleteJob(jobKey) ? "success" : "failed");
                }
            }
        }
    }

    public String startNewJobSchedule(
            String name,
            SystemType systemType,
            String cronString,
            ScheduleQuery scheduleQuery,
            ScheduleTask scheduleTask
    ) {
        String jobName = null;
        try {
            JobDataMap m = new JobDataMap();
            m.put(INPUT_DATA, scheduleQuery.getId());
            m.put("schedule_task", scheduleTask);
            m.put("common_repository", commonRepository);

            JobDetail job = JobBuilder.newJob(ScheduleJob.class)
                    .withIdentity(name, systemType.name())
                    .usingJobData(m)
                    .build();

            CronTrigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity("cron_" + name, systemType.name())
                    .withSchedule(CronScheduleBuilder.cronSchedule(cronString))
                    .forJob(name, systemType.name())
                    .startNow()
                    .build();

            Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.start();
            scheduler.scheduleJob(job, trigger);

            jobName = name; // Trả về tên job để lưu vào DB

        } catch (SchedulerException e) {
            e.printStackTrace();
        }
        return jobName;
    }
}
