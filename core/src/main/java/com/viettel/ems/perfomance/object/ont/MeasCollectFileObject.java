package com.viettel.ems.perfomance.object.ont;

import com.viettel.ems.perfomance.object.ont.MeasDataObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MeasCollectFileObject {
    private String userLabel;
    private LocalDateTime beginTime;
    private LocalDateTime endTime;
    private int duration;
    private List<MeasDataObject> measData;
}