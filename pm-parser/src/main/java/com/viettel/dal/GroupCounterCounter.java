package com.viettel.dal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GroupCounterCounter {
    private int counterID;
    private String name;
    private int counterCatID;
    private String code;
}
