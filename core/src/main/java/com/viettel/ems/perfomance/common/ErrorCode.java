package com.viettel.ems.perfomance.common;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
@RequiredArgsConstructor
public enum ErrorCode {
    NO_ERROR(0, "NO ERROR"),
    ERROR_INSERT_DB(1, "DATABASE INSERTION FAILED"),
    ERROR_WRONG_PARAMETER(2, "INVALID PARAMETER PROVIDED"),
    ERROR_NULL_DB_CONNECTION(3, "DATABASE CONNECTION IS NULL"),
    ERROR_DUPLICATE_RECORD(4, "DUPLICATE RECORD DETECTED"),
    ERROR_ENB_NOT_EXISTS(21, "ENB DOES NOT EXIST"),
    ERROR_MISSING_FIELD(22, "REQUIRED FIELD IS MISSING"),


    ERROR_NE_NOT_EXISTS(23,"ERROR_NE_NOT_EXISTS" ),
    ERROR_COUNTER_ID_NOT_EXISTS(24,"ERROR_COUNTER_ID_NOT_EXISTS" ),
    ERROR_FILE_COUNTER_EMPTY(25,"ERROR_FILE_COUNTER_EMPTY" ),
    ERROR_FILE_INVALID_NAME(26,"ERROR_FILE_INVALID_NAME" ),




    ERROR_UNKNOWN(100, "UNKNOWN ERROR OCCURRED");

    private final int code;
    private final String description;

    private static final Map<Integer, ErrorCode> CODE_MAP =
            Arrays.stream(values())
                    .collect(Collectors.toMap(ErrorCode::getCode, Function.identity()));

    public static ErrorCode fromCode(int code) {
        return CODE_MAP.get(code);
    }
}