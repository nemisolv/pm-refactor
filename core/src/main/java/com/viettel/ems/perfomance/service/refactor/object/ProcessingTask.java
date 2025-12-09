package com.viettel.ems.perfomance.service.refactor.object;

import com.viettel.ems.perfomance.object.FtpServerObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class ProcessingTask {
    // Dữ liệu để Insert DB
    private Map<String, List<UnifiedRecord>> data;
    
    // Thông tin để Move File sau khi Insert xong
    private String filePath;
    private String fileName;
    private FtpServerObject ftpServerObject;
}