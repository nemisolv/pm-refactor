package com.viettel.repository;

import com.viettel.dal.GroupCounterCounter;
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
public class GroupCounterCounterRepository {

    private final JdbcTemplate jdbcTemplate;

    private final NamedParameterJdbcTemplate namedJdbcTemplate;



    public List<GroupCounterCounter> getListGroupCounterCat() {
        try {
            List<GroupCounterCounter> result = jdbcTemplate.query(
            "Select c.id, c.name, cc.id as 'counter_cat_id', cc.code\n" + "FROM counter c, counter_cat cc\n"
            + "WHERE cc.id = c.counter_cat_id order by c.id",
            (rs, rowNum) -> new GroupCounterCounter(
                rs.getInt("id"),
                rs.getString("name"),
                rs.getInt("counter_cat_id"),
                rs.getString("code")
            )
            );
            return result;
        }catch(BadSqlGrammarException e) {
            log.error(e.getMessage());
            return new ArrayList<>();
        }
    }
    
}
