package com.viettel.dal;

import lombok.Builder;
import lombok.Data;
@Data
@Builder
public class NeLiteInfo {
    private Integer neID;
    private String name;
    private String ipAddress;
}
