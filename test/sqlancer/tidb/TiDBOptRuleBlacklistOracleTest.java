package sqlancer.tidb;

import sqlancer.tidb.oracle.TiDBOptRuleBlacklistOracle;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TiDBOptRuleBlacklistOracleTest {
    TiDBOptRuleBlacklistOracle oracle;


    @BeforeEach
    public void mockObjects() {
        oracle = mock(TiDBOptRuleBlacklistOracle.class);
        when(oracle.getSQLQueriesByGeneration()).thenReturn("select 1 from t0;");
    }

    @Test
    public void testOracle() {
        try{
            oracle.check();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
}
