package com.viettel.ems.perfomance.parser.better;

import com.viettel.ems.perfomance.object.CounterDataObject;
import com.viettel.ems.perfomance.service.refactor.object.UnifiedRecord;

import java.util.List;
import java.util.Map;

public interface ICounterParser {
    boolean support(String fileName);
    Map<String, List<UnifiedRecord>> parse(CounterDataObject dataObject);
}