
package sqlancer.tidb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.common.DecodedStmt;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBOptions;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.ast.TiDBText;
import sqlancer.tidb.gen.TiDBHintGenerator;
import sqlancer.tidb.visitor.TiDBVisitor;
import sqlancer.tidb.TiDBSQLParser;

public class TiDBPartitionTableOracle implements TestOracle<TiDBGlobalState> {
    private TiDBExpressionGenerator gen;
    public final TiDBGlobalState state;
    private TiDBSelect select;
    private final ExpectedErrors errors = new ExpectedErrors();


    public TiDBPartitionTableOracle(TiDBGlobalState globalState) {
        state = globalState;
        TiDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        List<String> queries = getSQLQueries();
        partition_table_oracle(queries);
    }

    
    public String getSQLQueriesByGeneration() {
        TiDBTables tables = state.getSchema().getRandomTableNonEmptyTables();
        gen = new TiDBExpressionGenerator(state).setColumns(tables.getColumns());
        select = new TiDBSelect();

        List<TiDBExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream().map(c -> new TiDBColumnReference(c))
                .collect(Collectors.toList()));
        select.setFetchColumns(fetchColumns);

        List<TiDBExpression> tableList = tables.getTables().stream().map(t -> new TiDBTableReference(t))
                .collect(Collectors.toList());
        List<TiDBExpression> joins = TiDBJoin.getJoins(tableList, state).stream().collect(Collectors.toList());
        select.setJoinList(joins);
        select.setFromList(tableList);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(gen.generateExpression());
        }

        String originalQueryString = TiDBVisitor.asString(select);
        return originalQueryString;
    }
    public List<String> getSQLQueries() {
        List<String> res = new ArrayList<String>();
        if(state.getRandomly().getBoolean()) {//get queries by generation
            for(int i = 0; i < ((TiDBOptions)state.getDbmsSpecificOptions()).queriesPerBatch; i ++ ) {
                res.add(getSQLQueriesByGeneration());
            }
        }else{                                //get queries from seed pool
            for(int i = 0; i < ((TiDBOptions)state.getDbmsSpecificOptions()).queriesPerBatch; i ++ ) {
                res.add(getSQLQueriesByGeneration());
            }
        }
        return res;
    }

    
    private void partition_table_oracle(List<String> queries) throws Exception {
        List<String> firstResult = new ArrayList<String>();
        List<String> secondResult = new ArrayList<String>();

        try{
            generateOracleTable();
        }catch(Exception e) {
            e.printStackTrace();
        }
        List<String> oracle_queries = generateOracleQueries();


  
        for(String query: queries) {
            firstResult.addAll(ComparatorHelper.getResultSetFirstColumnAsString(query, errors,state));
        }


        for(String query: oracle_queries) {
            secondResult.addAll(ComparatorHelper.getResultSetFirstColumnAsString(query, errors,state));
        }
        ComparatorHelper.assumeResultSetsAreEqualByBatch(firstResult, secondResult, queries, queries, state);//checkresults by batch

        
        state.getManager().incrementSelectQueryCount((long)queries.size());
    }
    private void removePartition(DecodedStmt stmt) {
        String str = stmt.getStmt();
        stmt.setStmt(str.substring(0, str.indexOf("partition")));
    }
    private void replaceTableNameWithOracleNameInStmt(DecodedStmt stmt) {
        for(String table: stmt.getTables()) {
            String new_name = table + "_oracle";
            stmt.setStmt(state.replaceStmtTableName(stmt.getStmt(), table, new_name));
        }
    }
    private void appendPartition(DecodedStmt stmt) {
        switch((int)state.getRandomly().getNotCachedInteger(0, 6)) {
            case(0):
                generateRangePartition(stmt);
                break;
            case(1):
                generateHashPartition(stmt);
                break;
            case(2):
                generateRangePartition2(stmt);
                break;
            case(3):
                generateHashPartition2(stmt);
                break;
            case(4):
                generateListPartition(stmt);
                break;
            case(5):
                generateKeyPartition(stmt);
                break;
            
        }
    }
    private void generateRangePartition(DecodedStmt stmt) {
        
    }
    private void generateOracleTable() {
        for(String stmt: state.getHistory()) {
            DecodedStmt decodedStmt = TiDBSQLParser.parse(stmt, state.getDatabaseName());
            decodedStmt.setStmt(decodedStmt.getStmt().toLowerCase());
            if(decodedStmt.getParseSuccess()) {
                if(decodedStmt.getStmtType() == DecodedStmt.stmtType.DDL) {
                    if(decodedStmt.getStmt().toLowerCase().contains("partition")) {
                        removePartition(decodedStmt);
                    }
                    appendPartition(decodedStmt);
                }else{
                    replaceTableNameWithOracleNameInStmt(decodedStmt);
                }
                state.executeStatement(new SQLQueryAdapter(decodedStmt.getStmt(), errors));
            }
        }
    }
}

