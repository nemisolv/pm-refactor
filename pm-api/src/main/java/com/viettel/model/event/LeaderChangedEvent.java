package com.viettel.model.event;


import org.springframework.context.ApplicationEvent;

public class LeaderChangedEvent extends ApplicationEvent {

    public LeaderChangedEvent(Object source) {
        super(source);
    }
}
