package com.viettel.ems.perfomance.object;

import java.util.ArrayList;
import java.util.List;

public class JDBCObject {
    private final List<Object> objects;

    public JDBCObject() {
        objects = new ArrayList<>();
    }
    public boolean addItem(Object item) {
        if(item == null) return false;
        objects.add(item);
        return true;
    }

    public Object getItem(int index) {
        if(objects == null) return null;
        return objects.get(index);
    }

    public int size() {
        if(objects == null) return 0;
        return objects.size();
    }
}
