package com.viettel.ems.perfomance.object.ont;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class Parameter  {
    String path;
    String code;
    Integer businessCode;
}