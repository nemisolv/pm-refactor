package com.viettel.ems.perfomance.object;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AlarmObject {
    private int alarmType;
    private String alarmID;
    private String alarmName;
    private String neName;
    private String severity;
    private Date triggerTime;
    private String detail;
    private String location;
    private String keyGen;
    private String expressionName;
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return String.format("[%s] %s (%s) Location %s. Time: %s. %s | Details: \n%s",
        neName, alarmName, alarmID, location, sdf.format(triggerTime), severity, detail
        );
    }
}