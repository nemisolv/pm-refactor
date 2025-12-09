package com.viettel.ems.perfomance.common;

import com.viettel.ems.perfomance.object.CounterDataObject;

@FunctionalInterface
public interface CounterFileHandler {
    void onSuccess(CounterDataObject counterDataObject);
}
