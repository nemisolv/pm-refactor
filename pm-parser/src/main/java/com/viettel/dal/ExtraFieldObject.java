package com.viettel.dal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ExtraFieldObject {
    private Integer id;
    private String columnCode;
    private String columnName;
    private String displayName;
    private String columnType;
    private Integer visible;
    private Integer crud;
    private Integer key;

    private String oneStepOutside;

    public ExtraFieldObject(int id, String columnCode, String columnName, String displayName, 
    String columnType, int visible, int crud, int key ) {
        this.id = id;
        this.columnCode = columnCode;
        this.columnName = columnName;
        this.displayName = displayName;
        this.columnType = columnType;
        this.visible = visible;
        this.crud = crud;
        this.key = key;
    }
}
