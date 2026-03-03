package sqlancer.cockroachdb;


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

import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;



public class CockroachDBMutatorTest {


    CockroachDBMutator mutator;
    CockroachDBMutator spyMutator;
    CockroachDBSchema schema;
    CockroachDBSchema spySchema;
    @BeforeEach
    private void init(){
        mutator = new CockroachDBMutator(null, null);
        List<CockroachDBTable> databaseTables = new ArrayList<CockroachDBTable>();
        schema = new CockroachDBSchema(databaseTables);
        spyMutator = Mockito.spy(mutator);
        spySchema = Mockito.spy(schema);
        
    }

    @Test
    public void testMutateDDL() {
        String sql = "create table t0 (c0 interval, c1 bool, constraint \"primary\" primary key(c0), family \"primary\" (c0, c1)) partition by range(c0)(partition p0 values from(6140) to (12359),partition p1 values from(12359) to (18597),partition p2 values from(18597) to (24803),partition p3 values from(24803) to (31010),partition p4 values from(31010) to (37159),partition p5 values from(37159) to (43364),partition p6 values from(43364) to (49506),partition p7 values from(49506) to (55662),partition p8 values from(55662) to (61894),partition p9 values from(61894) to (68070),partition p10 values from (68070) to (maxvalue));";
        System.out.println(spyMutator.extractColumnTypeFromStmt(sql, "c0"));
    }

}

