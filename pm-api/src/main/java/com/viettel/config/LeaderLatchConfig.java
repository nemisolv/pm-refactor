package com.viettel.config;

import com.viettel.event.LeaderChangedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
@Slf4j
public class LeaderLatchConfig {
    private final ApplicationEventPublisher eventPublisher;

    private String generateId(String instanceName, int port) {
        return instanceName + "-" + port + "-" + UUID.randomUUID().toString().substring(0,8);
    }

    @Bean(initMethod = "start")
    public CuratorFramework curatorFramework(@Value("${spring.zookeeper.connection-url}") String connection,
                                             @Value("${spring.zookeeper.max-retry:10}") int maxRetry
    ) {
        return CuratorFrameworkFactory.newClient(connection, new ExponentialBackoffRetry(1000, maxRetry));
    }




    @Autowired
    public LeaderLatchConfig(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }



    @Bean
    public LeaderLatch leaderLatch(@Value("${spring.zookeeper.leader-path:/ont-3/api}") String leaderpath,
                                   CuratorFramework curatorFramework,
                                   @Value("${spring.zookeeper.leader-event-delay:100000}") int leaderEventDelay,
                                   @Value("${server.port}") int port,
                                   @Value("${instance.name:unknown-insntance-name}") String instanceName
    ) {
        final String id = generateId(instanceName, port);
        LeaderLatch leaderLatch = new LeaderLatch(curatorFramework, leaderpath, id);
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                log.info("The current instance is leader");
                try {
                    Thread.sleep(leaderEventDelay);
                }catch(InterruptedException ex) {
                    log.error("Exception while electing leader");
                }

                log.debug("Leader chagend event should be fired now");
                eventPublisher.publishEvent(new LeaderChangedEvent(this));
                log.debug("Push event leader changed success!");
            }

            @Override
            public void notLeader() {
                log.info("The current instance is no longer leader");
            }
        });
        log.debug("initialiing leader latch with id {}", leaderLatch.getId());
        return leaderLatch;
    }



}
