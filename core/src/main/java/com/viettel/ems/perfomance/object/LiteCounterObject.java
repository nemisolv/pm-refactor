package com.viettel.ems.perfomance.object;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor@NoArgsConstructor
@Builder
public class LiteCounterObject {
    private int id;
    private String name;
    private int position;
}
