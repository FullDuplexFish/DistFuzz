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
        String sql = "CREATE TABLE t0 (c0 BOOL AS ((c1 < ALL (c1, c1, c1, c1))) STORED, c1 VARBIT(130) CHECK (false), FAMILY \"primary\" (c0, c1)) partition by range(c1)(partition p0 values from(6908) to (13855),partition p1 values from(13855) to (20812),partition p2 values from(20812) to (27770),partition p3 values from(27770) to (34755),partition p4 values from(34755) to (41698),partition p5 values from(41698) to (48666),partition p6 values from(48666) to (55621),partition p7 values from(55621) to (62607),partition p8 values from(62607) to (69562),partition p9 values from(69562) to (76563),partition p10 values from (76563) to (maxvalue));";
        System.out.println(spyMutator.extractColumnTypeFromStmt(sql, "c1"));
    }

}

