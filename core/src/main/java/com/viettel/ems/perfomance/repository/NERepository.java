package com.viettel.ems.perfomance.repository;

import com.viettel.ems.perfomance.object.NEObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@Slf4j
@RequiredArgsConstructor
public class NERepository {
    private final JdbcTemplate jdbcTemplate;

    public List<NEObject> findAllNeActive() {
        String sql = "SELECT id, name, ip_address, is_active FROM ne WHERE is_active = true";
        return jdbcTemplate.query(sql, (rs, _rowNum) -> NEObject.fromRs(rs));
    }
}
