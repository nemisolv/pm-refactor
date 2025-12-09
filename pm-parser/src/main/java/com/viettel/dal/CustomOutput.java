package com.viettel.dal;

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
