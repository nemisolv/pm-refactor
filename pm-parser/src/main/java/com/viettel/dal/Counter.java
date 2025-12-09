package com.viettel.dal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Counter {
    private Integer counterID;
    private String name;
    private Integer counterCatID;
    private String formula;


}
