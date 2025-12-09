package com.viettel.ems.perfomance.object.ont;

import lombok.Data;

@Data
public class MessageCPEObject {
    private String path;
    private String code;
    private String type;
    private String value;
    private Integer businessCode;
    private boolean isCounter;
    private int isExtraFieldKey = 0;

    public boolean isCounter () {
        return code.contains("Sent") || code.contains("Received");
    }
}