
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
import sqlancer.common.DecodedStmt;
import sqlancer.common.DecodedStmt.stmtType;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBOptions;
import sqlancer.tidb.TiDBSQLParser;
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

public class TiDBOptRuleBlacklistOracle implements TestOracle<TiDBGlobalState> {
    private TiDBExpressionGenerator gen;
    private final TiDBGlobalState state;
    private TiDBSelect select;
    private final ExpectedErrors errors = new ExpectedErrors();


    public TiDBOptRuleBlacklistOracle(TiDBGlobalState globalState) {
        state = globalState;
        TiDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        List<String> queries = getSQLQueries();
        opt_rule_blacklist_oracle(queries);
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
  
    private void clearOptBlacklist() throws Exception{
        state.executeStatement(new SQLQueryAdapter("delete from mysql.opt_rule_blacklist;"));
        state.executeStatement(new SQLQueryAdapter("admin reload opt_rule_blacklist;"));
        state.executeStatement(new SQLQueryAdapter("delete from mysql.expr_pushdown_blacklist;"));
        state.executeStatement(new SQLQueryAdapter("admin reload expr_pushdown_blacklist;"));
    }
    private void insertOptBlacklist() throws Exception {
        state.executeStatement(new SQLQueryAdapter("INSERT INTO mysql.opt_rule_blacklist VALUES(\"aggregation_push_down\"), (\"predicate_push_down\")," +
                                                        "(\"column_prune\"), (\"decorrelate\"), (\"aggregation_eliminate\"),(\"projection_eliminate\")," + 
                                                        "(\"max_min_eliminate\"),(\"outer_join_eliminate\"),(\"partition_processor\"),(\"topn_push_down\"),(\"join_reorder\");"));
        state.executeStatement(new SQLQueryAdapter("ADMIN reload opt_rule_blacklist;"));
        String[] exprs = {"AND","OR","NOT","XOR","&","~","|","^","<<",">>","<","<=","=","!=",">",">=","<=>","BETWEEN...AND...","COALESCE()","IN()","INTERVAL()","IS NOT NULL","IS NOT","IS","NULL","IS","ISNULL()","LIKE","NOT BETWEEN...AND...","NOT IN()","NOT LIKE","STRCMP()","+","-","*","/","DIV","%","ABS()","ACOS()","ASIN()","ATAN()","ATAN2()","ATAN()","CEIL()","CEILING()","CONV()","COS()","COT()","CRC32()","DEGREES()","EXP()","FLOOR()","LN()","LOG()","LOG10()","LOG2()","MOD()","PI()","POW()","POWER()","RADIANS()","RAND()","ROUND()","SIGN()","SIN()","SQRT()","CASE","IF()","IFNULL()","DATE()","DATE_FORMAT()","DATEDIFF()","DAYOFMONTH()","DAYOFWEEK()","DAYOFYEAR()","FROM_DAYS()","HOUR()","MAKEDATE()","MAKETIME()","MICROSECOND()","MINUTE()","MONTH()","MONTHNAME()","PERIOD_ADD()","PERIOD_DIFF()","SEC_TO_TIME()","SECOND()","SYSDATE()","TIME_TO_SEC()","TIMEDIFF()","WEEK()","WEEKOFYEAR()","YEAR()","ASCII()","BIT_LENGTH()","CHAR()","CHAR_LENGTH()","CONCAT()","CONCAT_WS()","ELT()","FIELD()","HEX()","LENGTH()","LIKE","LOWER()","LTRIM()","MID()","NOT LIKE","NOT REGEXP","REGEXP","REGEXP_INSTR()","REGEXP_LIKE()","REGEXP_REPLACE()","REGEXP_SUBSTR()","REPLACE()","REVERSE()","RIGHT()","RLIKE","RTRIM()","SPACE()","STRCMP()","SUBSTR()","SUBSTRING()","UPPER()","COUNT()","COUNT(DISTINCT)","SUM()","AVG()","MAX()","MIN()","VARIANCE()","VAR_POP()","STD()","STDDEV()","STDDEV_POP","VAR_SAMP()","STDDEV_SAMP()","MD5()","SHA1()","SHA()","UNCOMPRESSED_LENGTH()","CAST()","CONVERT()","UUID()"};
        String ban_exprs = "insert into mysql.expr_pushdown_blacklist values";
        for(int i = 0; i < exprs.length; i ++ ) {
            if(i > 0) ban_exprs += ",";
            ban_exprs += "('" + exprs[i] + "','tikv','')";
        }
        state.executeStatement(new SQLQueryAdapter(ban_exprs));
        state.executeStatement(new SQLQueryAdapter("ADMIN reload expr_pushdown_blacklist;"));
    }
    private void opt_rule_blacklist_oracle(List<String> queries) throws Exception {
        List<String> firstResult = new ArrayList<String>();
        List<String> secondResult = new ArrayList<String>();

        clearOptBlacklist();
        for(String query: queries) {
            firstResult.addAll(ComparatorHelper.getResultSetFirstColumnAsString(query, errors,state));
        }
        
        insertOptBlacklist();
        for(String query: queries) {
            secondResult.addAll(ComparatorHelper.getResultSetFirstColumnAsString(query, errors,state));
        }
        String assertionMessage = ComparatorHelper.assumeResultSetsAreEqualByBatch(firstResult, secondResult, queries, queries, state);//checkresults by batch
        
        
        if(assertionMessage != null) {
            state.addHistoryToSeedPool();
            throw new AssertionError(assertionMessage);
        }
        
        state.getManager().incrementSelectQueryCount((long)queries.size());
    }
}

