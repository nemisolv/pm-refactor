package com.viettel.repository;

import com.viettel.dal.GroupCounterCounter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@Repository
@RequiredArgsConstructor
public class GroupCounterRepository {

    private final JdbcTemplate jdbcTemplate;


    public List<GroupCounterCounter> getListGroupCounterCat() {
        try {
            List<GroupCounterCounter> resultSQL = jdbcTemplate.query(
                "SELECT C.id, C.name, cc.id as 'counter_cat_id', cc.code\n" +
                "FROM counter c, counter_cat cc\n" +
                "WHERE cc.id = c.counter_cat_id ORDER BY c.id",
                (rs, rowNum) -> new GroupCounterCounter(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("counter_cat_id"),
                    rs.getString("code")
                )
            );
            return resultSQL;
        } catch (BadSqlGrammarException e) {
            log.error(e.getMessage());
            return null;
        }
    }
}