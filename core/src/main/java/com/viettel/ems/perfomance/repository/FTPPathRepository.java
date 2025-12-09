package com.viettel.ems.perfomance.repository;

import com.viettel.ems.perfomance.object.FTPPathObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
@Slf4j
@RequiredArgsConstructor
public class FTPPathRepository {
    private final JdbcTemplate jdbcTemplate;
    public List<FTPPathObject> findAll() {
        try {
            String sql = "SELECT p.id, p.code, p.name, s.host, s.port, s.user_name, s.password, p.path from ftp_server s, ftp_path p  where s.id = p.ftp_server_id  order by s.host";
            return jdbcTemplate.query(sql, (rs, i) -> FTPPathObject.fromRs(rs));
        }catch (Exception e){
            log.error(e.getMessage());
            return new ArrayList<>();
        }
    }
}
