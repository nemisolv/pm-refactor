package com.viettel.ems.perfomance.object.ont;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ProcessDataONT {
    Map<String, List<Map<String, Object>>> dataPushClickhouses;
    MeasCollectFileObject data;
    String fileName;
    String productClass;
    boolean success;
}