package com.viettel.dal;

import lombok.Data;

import java.util.HashSet;
import java.util.Set;

@Data
public class QueryObject {
    private String sql;
    private Set<String> column = new HashSet<>();
}
