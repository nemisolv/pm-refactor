package com.viettel.ems.perfomance.object;

import lombok.Data;

@Data
public class RelationObject {
    private String freqRelation;
    private String cellRelation;

    public RelationObject() {
        freqRelation = null;
        cellRelation = null;
    }
}