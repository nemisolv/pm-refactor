package com.viettel.ems.perfomance.object;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;

@Data
@Builder
@AllArgsConstructor
public class FTPPathObject {
    private int id;
    private String code;
    private String name;
    private FtpServerObject ftpServerObject;
    private String path;




    public static FTPPathObject fromRs(ResultSet rs) throws SQLException {
       return new FTPPathObject(rs.getInt("id"), rs.getString("code"),
       rs.getString("name"),
       new FtpServerObject(rs.getString("host"),
       rs.getInt("port"), rs.getString("user_name"),
       rs.getString("password")        ),
       rs.getString("path")
       );




    }

}
