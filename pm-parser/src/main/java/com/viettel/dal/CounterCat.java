package com.viettel.dal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CounterCat {
    private int counterCatID;
    private String name;
    private int level;
    private int parentID;
    private String description;
    private String version;
    private String code;
}