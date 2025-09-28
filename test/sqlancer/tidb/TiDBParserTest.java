package sqlancer.tidb;

import sqlancer.common.DecodedStmt;
import sqlancer.tidb.oracle.TiDBOptRuleBlacklistOracle;
import org.junit.jupiter.api.Test;

import com.alibaba.fastjson.JSON;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TiDBParserTest {
    TiDBSQLParser parser;

    @BeforeEach
    public void setUp() {
        parser = new TiDBSQLParser();
    }

    @Test
    public void testSelect() {
        //DecodedStmt res = parser.parse("select t0.c1, t1.c2 from t0 natural join t1 where t0.c2 > 1", "database0");
        DecodedStmt res = parser.parse("select t0.c1, t1.c2 from t0 natural join t1 where t0.c2 > 1", "database0");
        System.out.println(res.getStmt());
        String str = JSON.toJSONString(res);
        System.out.println(str);
    }
}
