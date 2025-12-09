package com.viettel.ems.perfomance.performance;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class    CustomOutput<T> {
    T funcOutput;
    String customStringOutput;
}
