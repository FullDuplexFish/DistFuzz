
package sqlancer.tidb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
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

public class TiDBPlacementRuleOracle implements TestOracle<TiDBGlobalState> {
    private TiDBExpressionGenerator gen;
    private final TiDBGlobalState state;
    private TiDBSelect select;
    private final ExpectedErrors errors = new ExpectedErrors();

    private List<String> history;

    public TiDBPlacementRuleOracle(TiDBGlobalState globalState) {
        state = globalState;
        TiDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        List<String> queries = getSQLQueries();
        placement_rule_oracle(queries);
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
    List<String> getSQLQueries() {
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
    public void changePolicy(String chosen_table){
	    
        try{
                   state.executeStatement(new SQLQueryAdapter("ALTER TABLE " + chosen_table + " PLACEMENT POLICY=p" + Integer.toString(state.getRandomly().getInteger(1,3)), errors, true));
                state.getState().logStatement("ALTER TABLE " + chosen_table + " PLACEMENT POLICY=p" + Integer.toString(state.getRandomly().getInteger(1,3)));
        }catch(Exception e){
            e.printStackTrace();
        }
        int scheduled = 0;
         try{
             for(int i = 0; i < 10; i ++ ) {
                         SQLQueryAdapter q = new SQLQueryAdapter("SHOW PLACEMENT WHERE Target=\'TABLE " + state.getDatabaseName() + "." + chosen_table + "\';");
                         try (SQLancerResultSet rs = q.executeAndGet(state)) {
                         if (rs != null) {
                             while (rs.next()) {
                                 String sc_st = rs.getString(3);
                     if(sc_st.equals("SCHEDULED")){
                         scheduled = 1;
                         break;
                     }
                             }
                             }
                         } catch (Throwable e) {
                             e.printStackTrace();
                         }
                 if(scheduled == 1)
                 {
                     break;
                 }
             }
         }catch(Exception e){
             e.printStackTrace();
             
         }finally{
             if(scheduled == 0)
             {
                 state.getState().logStatement("NOT SCHEDULED!");
             }
         }
     }
    
    private void placement_rule_oracle(List<String> queries) throws Exception {
        List<String> firstResult = new ArrayList<String>();
        List<String> secondResult = new ArrayList<String>();

        //changePolicy();
        for(String query: queries) {
            firstResult.addAll(ComparatorHelper.getResultSetFirstColumnAsString(query, errors,state));
        }
        
        //changePolicy();
        for(String query: queries) {
            secondResult.addAll(ComparatorHelper.getResultSetFirstColumnAsString(query, errors,state));
        }
        ComparatorHelper.assumeResultSetsAreEqualByBatch(firstResult, secondResult, queries, queries, state);//checkresults by batch

        
        state.getManager().incrementSelectQueryCount((long)queries.size());
    }
}

