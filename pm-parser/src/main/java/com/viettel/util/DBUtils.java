package com.viettel.util;


public class DBUtils {
    public static String getFirstLettersTbl(String tableName) {
       if(tableName.contains("_")) {
        StringBuilder firstLeters = new StringBuilder();
        String [] words = tableName.split("_");

        for(String word : words) {
            if(!word.isEmpty()) {
                firstLeters.append(word.charAt(0));
            }
        }
        return firstLeters.toString();
       }else {
        return tableName;
       }
    }
}
