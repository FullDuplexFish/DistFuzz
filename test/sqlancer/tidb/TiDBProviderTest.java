package sqlancer.tidb;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import sqlancer.DBMSSpecificOptions;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.gen.TiDBTableGenerator;
import sqlancer.tidb.oracle.TiDBPlacementRuleOracle;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBProviderTest {
    TiDBProvider provider;
    TiDBGlobalState state;
    @BeforeEach
    private void init(){
        provider = new TiDBProvider();
        TiDBGlobalState originalState = new TiDBGlobalState();
        state = Mockito.spy(originalState);

        TiDBSchema schema = mock(TiDBSchema.class);
        MainOptions op = mock(MainOptions.class);
        Randomly ran = mock(Randomly.class);
        TiDBTables tables = mock(TiDBTables.class);
        List<TiDBOracleFactory> oracle = Arrays.asList(TiDBOracleFactory.QUERY_PARTITIONING);
        TiDBOptions dbmsop = mock(TiDBOptions.class);
        try{
            when(schema.getFreeTableName()).thenReturn("t0");
            when(op.getMaxExpressionDepth()).thenReturn(3);
            when(ran.getInteger()).thenReturn(5L);
            when(dbmsop.getTestOracleFactory()).thenReturn(oracle);
            // when(state.getSchema()).thenReturn(schema);
            // when(state.getOptions()).thenReturn(op);
            // when(state.getRandomly()).thenReturn(ran);
            // when(state.executeStatement(Mockito.any())).thenReturn(true);
            // when(state.getDbmsSpecificOptions()).thenReturn(dbmsop);
            Mockito.doReturn(schema).when(state).getSchema();
            Mockito.doReturn(op).when(state).getOptions();
            Mockito.doReturn(ran).when(state).getRandomly();
            Mockito.doReturn(true).when(state).executeStatement(Mockito.any());
            Mockito.doReturn(dbmsop).when(state).getDbmsSpecificOptions();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGenerateDatabase() {
        try(MockedStatic<TiDBVisitor> visitor = Mockito.mockStatic(TiDBVisitor.class);MockedConstruction<TiDBTableGenerator> generaotr = Mockito.mockConstruction(TiDBTableGenerator.class,
        (mock, context) -> when(mock.getQuery(state)).thenReturn(new SQLQueryAdapter("create table t0(c0 int)")))){
             visitor.when(() -> TiDBVisitor.asString(Mockito.any()))
              .thenReturn("1 + 1");
            //state.initHistory();
            provider.generateDatabase(state);
            System.out.println(state.getHistory());
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

}
