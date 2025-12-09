package com.viettel.ems.perfomance.object;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class NELiteObject {
    private String ipAddress;
    private int neId;
    private String neName;
    private boolean isRecvWarning;
}