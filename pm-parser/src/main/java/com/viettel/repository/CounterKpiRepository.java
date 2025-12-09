package com.viettel.repository;

import com.viettel.dal.Counter;
import com.viettel.dal.CounterCat;
import com.viettel.dal.KpiFormula;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;


@Repository
@Slf4j
@RequiredArgsConstructor
public class CounterKpiRepository {
    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedJdbcTemplate;



    public List<Counter> getAllListCounter(){
        String sql = "SELECT id, name, counter_cat_id, formula FROM counter WHERE is_kpi = 0";
        try {
            return jdbcTemplate.query(sql, (rs, _row) -> new Counter(rs.getInt("id"),
                                                        rs.getString("name").trim(),
                                                        rs.getInt("counter_cat_id"),
                                                        rs.getString("formula"))
                                                        );
        
        }catch( BadSqlGrammarException e) {
            log.error("getAllListCounter error: {}", e.getMessage());
            return new ArrayList<>();
        }
    }


    public List<CounterCat> getAllListCounterCat() {
    try {
        List<CounterCat> result = jdbcTemplate.query(
            "SELECT * FROM counter_cat",
            (rs, _rowNum) -> new CounterCat(
                rs.getInt("id"),
                rs.getString("name").trim(),
                rs.getInt("level"),
                rs.getInt("parent_id"),
                rs.getString("description"),
                rs.getString("version"),
                rs.getString("code")
            )
        );
        return result;
    } catch (BadSqlGrammarException e) {
        log.error(e.getMessage());
        return new ArrayList<>();
    }
}


   public List<KpiFormula> getAllListKpiFomularCaching() {
    try {
        List<KpiFormula> result = jdbcTemplate.query(
            "SELECT id, name, description, formula, counter_unit.unit_name as units, created_date, created_by, status, kpi_type, account_id, original_formula, is_peak_hour, arr_counter, is_single_group, arr_group, arr_kpi, sk_formula, sk_cat_id FROM counter " +
            "LEFT JOIN counter_unit ON counter.unit_id = counter_unit.unit_id WHERE is_kpi = 1",
            (rs, rowNum) -> new KpiFormula(
                rs.getInt("id"),
                rs.getString("name").trim(),
                rs.getString("description"),
                rs.getString("formula"),
                rs.getString("units"),
                rs.getString("created_date"),
                rs.getString("created_by"),
                rs.getInt("status"),
                rs.getInt("kpi_type"),
                rs.getInt("account_id"),
                rs.getString("original_formula"),
                rs.getBoolean("is_peak_hour"),
                rs.getString("arr_counter"),
                rs.getBoolean("is_single_group"),
                rs.getString("arr_group"),
                rs.getString("arr_kpi"),
                new ArrayList<>(),
                rs.getString("sk_formula"),
                rs.getInt("sk_cat_id")
            )
        );
        return result;
    } catch (BadSqlGrammarException e) {
        log.error(e.getMessage());
        return new ArrayList<>();
    }
}

public List<KpiFormula> getAllListKpiFomular() {
    try {
        List<KpiFormula> result = namedJdbcTemplate.query(
            "SELECT id, name, description, formula, counter_unit.unit_name as units, created_date, created_by, status, kpi_type, account_id, original_formula" + 
            ", is_peak_hour, arr_counter, is_single_group, arr_group, arr_kpi FROM counter left join counter_unit on counter.unit_id = counter_unit.unit_id where is_kpi = 1",
            (rs, rowNum) -> new KpiFormula(
                rs.getInt("id"),
                rs.getString("name").trim(),
                rs.getString("description"),
                rs.getString("formula"),
                rs.getString("units"),
                rs.getString("created_date"),
                rs.getString("created_by"),
                rs.getInt("status"),
                rs.getInt("kpi_type"),
                rs.getInt("account_id"),
                rs.getString("original_formula"),
                rs.getBoolean("is_peak_hour"),
                rs.getString("arr_counter"),
                rs.getBoolean("is_single_group"),
                rs.getString("arr_group"),
                rs.getString("arr_kpi"),
                new ArrayList<>(),
                null,
                -1
            )
        );
        return result;
    } catch (BadSqlGrammarException e) {
        log.error(e.getMessage());
        return new ArrayList<>();
    }
}



public String getNameCounterKPI(long counterID) {
    try {
        List<String> res = new ArrayList<>();
        jdbcTemplate.query("SELECT name, counter_unit.unit_name as units, is_kpi FROM counter " +
         "left join counter_unit on counter.unit_id = counter_unit.unit_id where id = ? ", (rs) -> {
            if(rs.getInt("is_kpi") == 1) {
                res.add(rs.getString("name").trim() + " (" + rs.getString("units") + ")");
            }
         }, counterID);
            return res.size()>0 ? res.get(0) : null;
        
            }catch(BadSqlGrammarException e) {
        log.error(e.getMessage());
        return null;
    }
}





}
