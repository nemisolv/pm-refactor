package com.viettel.ems.perfomance.object;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.ResultSet;

@Data
@AllArgsConstructor
public class FtpServerObject {
    private String host;
    private int port;
    private String username;
    private String password;

    public String getKey() {
        return String.format("%s_%d_%s_%s", host, port, username, password);
    }

    // public static FtpServerObject fromRs(ResultSet rs) throws java.sql.SQLException {
    //     return new FtpServerObject(
    //             rs.getString("host"),
    //             rs.getInt("port"),
    //             rs.getString("user_name"),
    //             rs.getString("password")
    //     );
    // }
}
