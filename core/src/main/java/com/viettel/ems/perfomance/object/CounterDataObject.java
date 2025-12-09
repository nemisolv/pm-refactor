package com.viettel.ems.perfomance.object;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class CounterDataObject {
    private FtpServerObject ftpServerObject;
    private String path;
    private String fileName;
    private ByteBuffer bufferData;
    private List<String> lineData;
}
