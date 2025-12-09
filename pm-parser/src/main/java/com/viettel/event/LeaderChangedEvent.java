package com.viettel.event;


import org.springframework.context.ApplicationEvent;

public class LeaderChangedEvent extends ApplicationEvent {

    public LeaderChangedEvent(Object source) {
        super(source);
    }
}