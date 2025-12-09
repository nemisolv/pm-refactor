package com.viettel.dal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResponseData {
    private List<ReportStatisticKPI> data;
    private int total;
    private int offset; 

   
}
