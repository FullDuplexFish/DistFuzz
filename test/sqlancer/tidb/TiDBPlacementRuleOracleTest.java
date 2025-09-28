package sqlancer.tidb;

import sqlancer.common.DecodedStmt;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.oracle.TiDBOptRuleBlacklistOracle;
import sqlancer.tidb.oracle.TiDBPlacementRuleOracle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class TiDBPlacementRuleOracleTest {
    TiDBPlacementRuleOracle oracle;
    TiDBPlacementRuleOracle spyOracle;
    Set<String> debug_tables;


    @BeforeEach
    public void mockObjects() {
        
        TiDBGlobalState state = mockGlobalState();
        List<String> queries = new ArrayList<String>();
        queries.add("select c1, c2 from t1 natural join t2 where t1.c1 = 1");
        queries.add("select c2 from t2");
        queries.add("select c1");
        
        
        //oracle = mock(TiDBPlacementRuleOracle.class);
        oracle = new TiDBPlacementRuleOracle(state);
        spyOracle = Mockito.spy(oracle);
    
        //Mockito.when(oracle.getSQLQueries()).thenReturn(queries);
        Mockito.doReturn(queries).when(spyOracle).getSQLQueries();
        Mockito.doNothing().when(spyOracle).changePolicy("t1");
        Mockito.doNothing().when(spyOracle).changePolicy("t2");
    }
    private TiDBGlobalState mockGlobalState() {
        TiDBGlobalState state = mock(TiDBGlobalState.class);
        when(state.getDatabaseName()).thenReturn("database0");
        return state;
    }

    @Test
    public void testOracle() {
        try{
            List<String> queries = new ArrayList<String>();
            queries.add("select c1, c2 from t1 natural join t2 where t1.c1 = 1");
            queries.add("select c2 from t2");
            queries.add("select c1");
            spyOracle.changePolicyForQueires(queries);
            //System.out.println(spyOracle.state.getDatabaseName());
            for(String table: spyOracle.debug_tables) {
                System.out.println(table);
            }
            // for(DecodedStmt q: spyOracle.debug_decodedStmt) {
            //     System.out.println(q);
            // }
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testChangePolicy() {
        try{
            List<String> queries = new ArrayList<String>();
            queries.add("select c1, c2 from t1 natural join t2 where t1.c1 = 1");
            queries.add("select c2 from t2");
            queries.add("select c1");
            Set<String> tables = new HashSet<String>();
            for(String str: queries) {
                DecodedStmt decodedStmt = TiDBSQLParser.parse(str, "database0");
                System.out.println(decodedStmt.getTables());
                if(decodedStmt.getParseSuccess() && decodedStmt.getTables() != null) {
                    for(String table: decodedStmt.getTables()) {
                        tables.add(table);
                    }
                }
            }
            debug_tables = tables;
            System.out.println(debug_tables);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
}
