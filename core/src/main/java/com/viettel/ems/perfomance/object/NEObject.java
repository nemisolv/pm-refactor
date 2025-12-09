package com.viettel.ems.perfomance.object;

import lombok.*;

import java.sql.ResultSet;
import java.sql.SQLException;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NEObject {
    private int id;
    private String name;
    private String ipAddress;
    private int status;
    private boolean isActive;
    private String vendorName;

     public static  NEObject fromRs(ResultSet rs) throws SQLException {
         NEObject neObject = new NEObject();
         neObject.setId(rs.getInt("id"));
         neObject.setName(rs.getString("name"));
         neObject.setIpAddress(rs.getString("ip_address"));
         neObject.setStatus(rs.getInt("is_active"));
         neObject.setActive(rs.getBoolean("is_active"));
 //        neObject.setVendorName(rs.getString("vendor_name"))
         ;
         return neObject;
     }
}