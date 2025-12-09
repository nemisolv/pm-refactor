package com.viettel.ems.perfomance.object;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CounterInfoObject {
    private FtpServerObject ftpServerObject;
    private String path;
    private String fileName;
}
