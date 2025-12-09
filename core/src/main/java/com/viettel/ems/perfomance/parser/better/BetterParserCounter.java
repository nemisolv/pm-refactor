package com.viettel.ems.perfomance.parser.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viettel.ems.perfomance.object.*;
import com.viettel.ems.perfomance.service.refactor.object.UnifiedRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class BetterParserCounter {

    private final List<ICounterParser> parsers = new ArrayList<>();

    // Constructor nhận data và init các strategy
    public BetterParserCounter(
            Map<String, NEObject> neMap,
                               Map<Integer, CounterCounterCatObject> counterCounterCatObjectMap,
                               Map<Integer, HashMap<String, ExtraFieldObject>> efMap,
                               HashMap<String, CounterConfigObject> configMap
    ) {
        // Init Strategies
        parsers.add(new OranCsvParser(neMap, counterCounterCatObjectMap, efMap, configMap));
        parsers.add(new CapnProtoParser(neMap, counterCounterCatObjectMap, efMap));
        parsers.add(new OntJsonParser(neMap, counterCounterCatObjectMap, efMap, new ObjectMapper()));
        // ...
    }

    public Map<String, List<UnifiedRecord>> parseCounter(CounterDataObject preCounter) {
        String fileName = preCounter.getFileName().toUpperCase();
        
        // Tìm parser phù hợp
        for (ICounterParser parser : parsers) {
            if (parser.support(fileName)) {
                return parser.parse(preCounter);
            }
        }
        
        log.warn("No parser found for file: {}", fileName);
        return Collections.emptyMap();
    }
}