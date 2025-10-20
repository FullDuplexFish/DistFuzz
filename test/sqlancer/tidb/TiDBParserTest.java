package sqlancer.tidb;

import sqlancer.common.DecodedStmt;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.oracle.TiDBOptRuleBlacklistOracle;
import sqlancer.tidb.visitor.TiDBVisitor;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.alibaba.fastjson.JSON;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TiDBParserTest {
    TiDBSQLParser parser;
    TiDBGlobalState state;

    @BeforeEach
    public void setUp() {
        parser = new TiDBSQLParser();
        
        state = mock(TiDBGlobalState.class);
        TiDBTable table = mock(TiDBTable.class);
        TiDBSchema schema = mock(TiDBSchema.class);
        List<TiDBTable> list = new ArrayList<TiDBTable>();
        list.add(table);
        
        try{
            when(table.getName()).thenReturn("table0");
           

            when(schema.getDatabaseTables()).thenReturn(list);
            
            when(state.getSchema()).thenReturn(schema);
            parser.setGlobalState(state);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSelect() {
        //DecodedStmt res = parser.parse("select t0.c1, t1.c2 from t0 natural join t1 where t0.c2 > 1", "database0");
        DecodedStmt res = parser.parse("select t0.c1, t1.c2 from t0 natural join t1 where t0.c2 > 1", "database0");
        System.out.println(res.getStmt());
        String str = JSON.toJSONString(res);
        System.out.println(str);
    }
    @Test
    public void testCreate() {
        //DecodedStmt res = parser.parse("select t0.c1, t1.c2 from t0 natural join t1 where t0.c2 > 1", "database0");
        DecodedStmt res = parser.parse("create table t0(c0 int, c1 double)", "database0");
        System.out.println(res.getStmt());
        String str = JSON.toJSONString(res);
        System.out.println(str);
    }
    @Test
    public void testRefineSQL() {
        //DecodedStmt res = parser.parse("select t0.c1, t1.c2 from t0 natural join t1 where t0.c2 > 1", "database0");
        //DecodedStmt res = parser.parse("select t0.c1, t1.c2 from t0 natural join t1 where t0.c2 > 1", "database0");
        try(MockedStatic<TiDBSchema> visitor = Mockito.mockStatic(TiDBSchema.class)) {
            
            TiDBColumn column = mock(TiDBColumn.class);
            List<TiDBColumn> list2 = new ArrayList<TiDBColumn>();
            list2.add(column);
            when(column.getName()).thenReturn("column0");
            visitor.when(() -> TiDBSchema.getTableColumns(Mockito.any(), Mockito.any()))
              .thenReturn(list2);
            String str = "select t0.c1, t1.c2 from t0 natural join t1 where t0.c2 > 1";
       
            System.out.println("after refine:" + parser.refineSQL(str));
        }catch(Exception e) {
            e.printStackTrace();
        }
        
    }
}
