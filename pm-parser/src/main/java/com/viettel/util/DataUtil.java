package com.viettel.util;

import java.util.Collection;

public class DataUtil {
    public static boolean isNotNullNotEmpty(Collection collection) {
        return collection != null && !collection.isEmpty();
    }
}
