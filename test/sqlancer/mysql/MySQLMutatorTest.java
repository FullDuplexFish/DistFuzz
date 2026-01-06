package sqlancer.mysql;


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
import sqlancer.mysql.MySQLSchema.MySQLTables;


public class MySQLMutatorTest {


    MySQLGlobalState state;
    MySQLMutator mutator;
    @BeforeEach
    private void init(){
        
        MySQLGlobalState originalState = new    MySQLGlobalState();
        state = Mockito.spy(originalState);

        Main.QueryManager manager = mock(Main.QueryManager.class);
        

        MySQLSchema schema = mock(MySQLSchema.class);
        MainOptions op = mock(MainOptions.class);
        Randomly ran = mock(Randomly.class);
        MySQLTables tables = mock(MySQLTables.class);
        //List<TiDBOracleFactory> oracle = Arrays.asList(TiDBOracleFactory.QUERY_PARTITIONING);
        MySQLOptions dbmsop = mock(MySQLOptions.class);
        List<String> creates = new ArrayList<String>();
        creates.add("create table t0(c1 int)");
        creates.add("create table t1(c1 int) partition by hash(c1) partitions 7");
        creates.add("create table t3(c0 float zerofill check (c0) default null );");
        creates.add("insert into t0 values(1)");
        List<String> queries = new ArrayList<String>();
        queries.add("select t1.c1, t0.c1 from t0 natural join t1 where t0.c1 > 0");


        try{
            when(schema.getFreeTableName()).thenReturn("t0");
            when(op.getMaxExpressionDepth()).thenReturn(3);
            when(ran.getInteger()).thenReturn(5L);
            when(ran.getString()).thenReturn("asdasd");
            
            when(ran.getDouble()).thenReturn(1.5);
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



        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMutateDDL() {
        List<String> tmp = new ArrayList<String>();
        tmp.add("100");
        try(MockedStatic<ComparatorHelper> visitor = Mockito.mockStatic(ComparatorHelper.class)){
            visitor.when(() -> ComparatorHelper.getResultSetFirstColumnAsString(Mockito.any(),Mockito.any(),Mockito.any()))
              .thenReturn(tmp);
            //state.initHistory();
            //mutator = new MySQLMutator(state, "create table t3(c0 tinyint, c1 double, c2 varchar(255));");
            //mutator = new MySQLMutator(state, "select c1 from t0 where c2 > 1;");
            //mutator = new MySQLMutator(state, "create table if not exists t1(c0 tinyint(245) zerofill  storage disk, c1 float  comment 'asdf'  unique key  column_format default storage disk null, c2 float   comment 'asdf'  column_format dynamic storage disk )");
            //System.out.println("yeah");
            // mutator = new MySQLMutator(state, "SELECT IF(CAST(\"T4<\" AS SIGNED),  EXISTS (SELECT 1), t0.c0) AS ref0 FROM t0 LIMIT 206024709770821125 OFFSET 5705474512207646344");
            //System.out.println(mutator.mutateDQL());

 
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

}

