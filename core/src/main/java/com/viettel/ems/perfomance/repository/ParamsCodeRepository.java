package com.viettel.ems.perfomance.repository;

import com.viettel.ems.perfomance.common.Constant;
import com.viettel.ems.perfomance.common.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
@RequiredArgsConstructor
public class ParamsCodeRepository {
    private final JdbcTemplate jdbcTemplate;

    public void updatePMCoreProcessKey(String localInstanceKey) {
        try {
            String sql = "UPDATE params_code SET pvalue = ? WHERE pkey = 'pm_core_process_id';";
            executeQuery(sql, new String[]{localInstanceKey}, 1);
        }catch (Exception e){
            log.error(e.getMessage());
        }
    }


    public String getPMCoreProcessKey(String localInstanceKey) {
        try {
            return jdbcTemplate.queryForObject(
                    "Select pvalue from params_code where pkey = 'pm_core_process_id';", null, String.class
            );
        }catch (Exception e){
            log.error(e.getMessage());
            if(e.getMessage().contains("Incorrect result size: expected 1, actual 0")) {
                addParamCodeProcessKey(localInstanceKey);
                return getPMCoreProcessKey(localInstanceKey);
            }
            return null;
        }
    }

    private void addParamCodeProcessKey(String localInstanceKey) {
        try {
            String sql = "INSERT INTO params_code(type, pkey, pvalue, description) VALUES (?, ?, ?, ?)";
            executeQuery(sql, new String[]{"pmcore", "pm_core_process_id", localInstanceKey, "<ip>_<processid>"},1);
        }catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    private ErrorCode executeQuery(String sql, String[] instanceKey, int retry ) {
        ErrorCode result = ErrorCode.NO_ERROR;
        try {
            jdbcTemplate.update(sql, instanceKey);
        }catch (Exception e) {
            log.error(e.getMessage(), e);
            if(retry < Constant.MAX_RETRY_DB) {
                executeQuery(sql, instanceKey, retry + 1);
            }
            log.error("FAILED to update instance: {}", sql, e);
            result = ErrorCode.ERROR_INSERT_DB;
        }
        return result;
    }
}
