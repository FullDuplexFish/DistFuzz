package sqlancer.tidb;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import sqlancer.ComparatorHelper;
import sqlancer.DBMSSpecificOptions;
import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.gen.TiDBTableGenerator;
import sqlancer.tidb.oracle.TiDBPartitionTableOracle;
import sqlancer.tidb.oracle.TiDBPlacementRuleOracle;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBPartitionTableOracleTest {

    TiDBPartitionTableOracle oracle;
    TiDBPartitionTableOracle spyOracle;
    TiDBGlobalState state;
    @BeforeEach
    private void init(){
        
        TiDBGlobalState originalState = new TiDBGlobalState();
        state = Mockito.spy(originalState);
        oracle = new TiDBPartitionTableOracle(state);
        spyOracle = Mockito.spy(oracle);
        Main.QueryManager manager = mock(Main.QueryManager.class);
        

        TiDBSchema schema = mock(TiDBSchema.class);
        MainOptions op = mock(MainOptions.class);
        Randomly ran = mock(Randomly.class);
        TiDBTables tables = mock(TiDBTables.class);
        //List<TiDBOracleFactory> oracle = Arrays.asList(TiDBOracleFactory.QUERY_PARTITIONING);
        TiDBOptions dbmsop = mock(TiDBOptions.class);
        List<String> creates = new ArrayList<String>();
        creates.add("create table t0(c1 int)");
        creates.add("create table t1(c1 int) partition by hash(c1) partitions 7");
        creates.add("insert into t0 values(1)");
        List<String> queries = new ArrayList<String>();
        queries.add("select t1.c1, t0.c1 from t0 natural join t1 where t0.c1 > 0");


        try{
            when(schema.getFreeTableName()).thenReturn("t0");
            when(op.getMaxExpressionDepth()).thenReturn(3);
            when(ran.getInteger()).thenReturn(5L);
            //when(dbmsop.getTestOracleFactory()).thenReturn(oracle);
     
            Mockito.doReturn(schema).when(state).getSchema();
            Mockito.doReturn(op).when(state).getOptions();
            Mockito.doReturn(ran).when(state).getRandomly();
            Mockito.doReturn(true).when(state).executeStatement(Mockito.any());
            Mockito.doReturn(dbmsop).when(state).getDbmsSpecificOptions();
            Mockito.doReturn(creates).when(state).getHistory();
            Mockito.doReturn("database0").when(state).getDatabaseName();
            Mockito.doNothing().when(manager).incrementSelectQueryCount();
            Mockito.doReturn(manager).when(state).getManager();
            
            Mockito.doReturn("c1").when(state).getRandomIntColumnString(Mockito.any());
            Mockito.doReturn("c1;NUM").when(state).getRandomColumnStrings(Mockito.any());


            Mockito.doReturn(queries).when(spyOracle).getSQLQueries();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTiDBPartitionTableOracle() {
        List<String> tmp = new ArrayList<String>();
        tmp.add("100");
        try(MockedStatic<ComparatorHelper> visitor = Mockito.mockStatic(ComparatorHelper.class)){
            visitor.when(() -> ComparatorHelper.getResultSetFirstColumnAsString(Mockito.any(),Mockito.any(),Mockito.any()))
              .thenReturn(tmp);
            //state.initHistory();
            spyOracle.check();
            System.out.println(state.getHistory());

 
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

}
