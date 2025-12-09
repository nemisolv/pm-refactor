package com.viettel.ems.perfomance.object.ont;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.AccessType;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;

@Getter
@Setter
public class CommonParam {

    @CustomMark(sqlColumnName  = "id", clazz = Integer.class)
    private Integer id;

    @CustomMark(sqlColumnName = "param_code")
    private String paramCode;

    @CustomMark(sqlColumnName = "key")
    private String key;

    @CustomMark(sqlColumnName = "val")
    private String val;

    @CustomMark(sqlColumnName = "description")
    private String description;


    @CustomMark(sqlColumnName = "type")
    private String type;

    public CommonParam() {

    }

    public static CommonParam toDTO(ResultSet rs) {
        CommonParam dto = new CommonParam();
        for(Field f : CommonParam.class.getDeclaredFields()) {
            try {   
                f.setAccessible(true);
                CustomMark ctm = f.getAnnotation(CustomMark.class);
                f.set(dto, rs.getObject(ctm.sqlColumnName(), ctm.clazz()));
            }catch(SQLException | IllegalAccessException throwables) {

            }
        }
        return dto;
    }

    @AccessType(AccessType.Type.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CustomMark {
        String sqlColumnName() default "";
        Class clazz() default String.class;
    }


}