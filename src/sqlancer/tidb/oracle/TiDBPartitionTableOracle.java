
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
        errors.add("already exists");
        errors.add("is of a not allowed type for this type of partitioning");
        errors.add("BLOB/TEXT column 'c0' used in key specification without a key length");
        errors.add("Generated column can refer only to generated columns defined prior to it");
        errors.add("Global Index is needed for index");
        errors.add("Duplicate key name");
        errors.add("Unsupported modify column");
        errors.add("check that column/key exists");
        errors.add("Multiple primary key defined");

        TiDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        partition_table_oracle();
    }

    
    private void partition_table_oracle() throws Exception {
        // List<String> firstResult = new ArrayList<String>();
        // List<String> secondResult = new ArrayList<String>();

        try{
            generateOracleTable();
        }catch(Exception e) {
            e.printStackTrace();
        }
        List<String> queries = state.mutateSQLQueries(state.getSQLQueries());
        System.out.println("queries are:");
        for(String str: queries) {
            System.out.println(str);
        }
        System.out.println("===");
        List<String> oracle_queries = new ArrayList<String>();
        for(String query: queries) {
            DecodedStmt stmt = TiDBSQLParser.parse(query, state.getDatabaseName());
            String new_query = stmt.getStmt();
            if(stmt.getTables().size() > 0) {
                for(String table: stmt.getTables()) {
                    new_query = state.replaceStmtTableName(new_query, table, table + "_oracle");
                    //System.out.println(new_query);
                }
            }
            oracle_queries.add(new_query);
        }

        System.out.println("oracle queries are:");
        for(String str: oracle_queries) {
            System.out.println(str);
        }
        System.out.println("===");


  
        /*for(String query: queries) {
            firstResult.addAll(ComparatorHelper.getResultSetFirstColumnAsString(query, errors,state));
        }


        for(String query: oracle_queries) {
            //System.out.println("mutated query:" + query);
            secondResult.addAll(ComparatorHelper.getResultSetFirstColumnAsString(query, errors,state));
        }
        String assertionMessage = ComparatorHelper.assumeResultSetsAreEqualByBatch(firstResult, secondResult, queries, oracle_queries, state);//checkresults by batch
        if(assertionMessage != null) {
            state.addHistoryToSeedPool();
            state.getManager().incrementSelectQueryCount((long)queries.size());
            throw new AssertionError(assertionMessage);
        }
        
        state.getManager().incrementSelectQueryCount((long)queries.size());*/
        for(int i = 0; i < queries.size(); i ++ ) {
            String query1 = queries.get(i);
            String query2 = oracle_queries.get(i);
            List<String> firstResult = ComparatorHelper.getResultSetFirstColumnAsString(query1, errors,
                state);
            List<String> secondResult = ComparatorHelper.getResultSetFirstColumnAsString(query2, errors,
                state);
            String msg = ComparatorHelper.assumeResultSetsAreEqualByBatch(firstResult, secondResult, List.of(query1), List.of(query2),
                state);
            state.getManager().incrementSelectQueryCount();
            if(msg != null) {
                state.addHistoryToSeedPool();
                throw new AssertionError(msg);
            }
        }
    }
    private void removePartition(DecodedStmt stmt) {
        String str = stmt.getStmt();
        stmt.setStmt(str.substring(0, str.indexOf("partition")));
    }
    private void replaceTableNameWithOracleNameInStmt(DecodedStmt stmt) {
        if(stmt.getTables() == null) {
            return;
        }
        for(String table: stmt.getTables()) {
            String new_name = table + "_oracle";
            stmt.setStmt(state.replaceStmtTableName(stmt.getStmt(), table, new_name));
        }
    }
    private String trimString(String str) {
        str = str.trim();
        if(str.endsWith(";")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }
    private void appendPartition(DecodedStmt stmt) {
        System.out.println("appending partitions");
        stmt.setStmt(trimString(stmt.getStmt()));

        switch((int)state.getRandomly().getNotCachedInteger(0, 1)) {
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
    public void generateKeyPartition(DecodedStmt stmt) {
        if(stmt.getTables().size() == 0) {
            return;
        }
        String str = stmt.getStmt();
        String table_name = stmt.getTables().get(0);
        String new_name = table_name + "_oracle";
        str = state.replaceStmtTableName(str, table_name, new_name);
        String col = state.getRandomColumnStrings(table_name);
        col = col.split(";")[0];
        if(col == null || Randomly.getBooleanWithRatherLowProbability()) {
            col = "";
        }
        int pcnt = (int)Randomly.getNotCachedInteger(10, 100);
        str += " partition by key(";
        str += col;
        str += ") partitions ";
        str += String.valueOf(pcnt);
        stmt.setStmt(str);
        return;
    }
    public void generateListPartition(DecodedStmt stmt) {
        if(stmt.getTables().size() == 0) {
            return;
        }
        String str = stmt.getStmt();
        String table_name = stmt.getTables().get(0);
        String new_name = table_name + "_oracle";
        str = state.replaceStmtTableName(str, table_name, new_name);
        String cols_and_types = state.getRandomColumnStrings(table_name);
        String[] tmp = cols_and_types.split(";");
        String[] cols = tmp[0].split(",");
        String[] types = tmp[1].split(",");
        /*state.getLogger().writeCurrent("print types");
        for(int i = 0 ; i < types.length; i ++ ) {
            state.getLogger().writeCurrent(types[i]);
        }*/
        str += " partition by list columns(" + tmp[0] + ") (";
        SQLQueryAdapter q = new SQLQueryAdapter("select " + tmp[0] + " from " + table_name);
        List<String> res_set = new ArrayList<String>();
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            if (rs != null) {
                String cur = "";
                while (rs.next()) {

                    for(int i = 0; i < cols.length; i ++ ) {
                        if(i > 0) cur += ",";
                        if(types[i].equals("NUM"))
                            cur += String.valueOf(rs.getObject(cols[i]));
                        else
                            cur += "\'" + String.valueOf(rs.getObject(cols[i])) + "\'";
                    }
                    if(!res_set.contains(cur))
                        res_set.add(cur);
                    cur = "";
                }
                //Collections.shuffle(res_set);

            }
        } catch (Throwable e) {
            e.printStackTrace();
        } 
        int st = (int)Randomly.getNotCachedInteger(0, res_set.size());
        int pcnt = (int)Randomly.getNotCachedInteger(0, res_set.size());
        for(int i = 0; i < pcnt; i ++ ) {

            str += " partition p" + String.valueOf(i) + " values in (" + res_set.get(st) + "),";
            st ++ ;
            st %= res_set.size();
        }

        str += " partition pmax values in (default));";
        stmt.setStmt(str);
        return;
        

    }
    public void generateRangePartition2(DecodedStmt stmt) {
        if(stmt.getTables().size() == 0) {
            return;
        }
        String str = stmt.getStmt();
        String table_name = stmt.getTables().get(0);
        String new_name = table_name + "_oracle";
        str = state.replaceStmtTableName(str, table_name, new_name);
        int low = (int)Randomly.getNotCachedInteger(10, 100000);
        int high = (int)Randomly.getNotCachedInteger(low + 10000, low + 1000000);
        int pcnt = (int)Randomly.getNotCachedInteger(3, 10);
        String q = "split table " + table_name + "_oracle between (" + low + ") and (" + high + ") regions " + pcnt + ";";
        str += ";" + q;
        stmt.setStmt(str);
        return;
    }
    private void generateRangePartition(DecodedStmt stmt) {
        if(stmt.getTables().size() == 0) {
            return;
        }
        String str = stmt.getStmt();
        String table_name = stmt.getTables().get(0);
        String col = state.getRandomIntColumnString(table_name);
        //System.out.println(stmt + " " + table_name);
        String new_name = table_name + "_oracle";
        str = state.replaceStmtTableName(str, table_name, new_name);
        
        if(col == null) {//no int type column
            stmt.setStmt(str);
            return;
        }
        int range = (int)Randomly.getNotCachedInteger(10, 10000);
        int cnt = (int)Randomly.getNotCachedInteger(10, 30);
        str += " partition by range(" + col + ")(";
        int bound = range;
        for(int i = 0; i < cnt; i ++ ) {
            str += "partition p" + String.valueOf(i) + " values less than(" + String.valueOf(bound) + "),";

            bound += range + (int)Randomly.getNotCachedInteger(0, 100);
        }
        str += "partition p" + String.valueOf(cnt) + " values less than maxvalue);";
        
        stmt.setStmt(str);
        System.out.println("trigger range oracle " + str);
        return;
        
    }
    public void generateHashPartition2(DecodedStmt stmt) {
        if(stmt.getTables().size() == 0) {
            return;
        }
        String table_name = stmt.getTables().get(0);
        String res = "";
        String col = state.getRandomIntColumnString(table_name);
        
        String new_name = table_name + "_oracle";
        res += "drop table if exists " + new_name + ";";
        res += "create table " + new_name + " like " + table_name + ";";
        res += "insert into " + new_name + " select * from " + table_name + ";";
        if(col == null) {
            stmt.setStmt(res);
            return;
        }
        res += "alter table " + new_name + " partition by hash(" + col + ") partitions " + String.valueOf(state.getRandomly().getInteger(1, 15)) + ";";
        res += "alter table " + new_name + " partition by hash(" + col + ") partitions " + String.valueOf(state.getRandomly().getInteger(1, 15)) 
            + " update indexes (`primary` global)";
        stmt.setStmt(res);
        return;

    }

    private void generateHashPartition(DecodedStmt stmt) {
        if(stmt.getTables().size() == 0) {
            return;
        }
        String str = stmt.getStmt();
        String table_name = stmt.getTables().get(0);
        String new_name = table_name + "_oracle";
        str = state.replaceStmtTableName(str, table_name, new_name);
        String col = state.getRandomIntColumnString(table_name);
        if(col == null) {
            stmt.setStmt(str);
            return;
        }
        int pcnt = (int)Randomly.getNotCachedInteger(10, 100);
        str += " partition by hash(";
        str += col;
        str += ") partitions ";
        str += String.valueOf(pcnt);
        stmt.setStmt(str);
        return;

    }

    private void generateOracleTable() {

        if(state.historyIsUsed == true) return;
        for(String stmt: state.getHistory()) {
            DecodedStmt decodedStmt = TiDBSQLParser.parse(stmt, state.getDatabaseName());
            decodedStmt.setStmt(decodedStmt.getStmt().toLowerCase());
            if(decodedStmt.getParseSuccess()) {
                if(decodedStmt.getStmtType() == DecodedStmt.stmtType.DDL) {
                    if(decodedStmt.getStmt().toLowerCase().contains("like")) {
                        replaceTableNameWithOracleNameInStmt(decodedStmt);
                    }else if(decodedStmt.getStmt().toLowerCase().contains("create index")) {//create index takes too much time
                        continue;
                    }
                    else{
                        if(decodedStmt.getStmt().toLowerCase().contains("partition")) {
                        removePartition(decodedStmt);
                        }
                        appendPartition(decodedStmt);
                    }
                    
                    
                }else{
                    if(!decodedStmt.getStmt().contains("_oracle")) {
                        replaceTableNameWithOracleNameInStmt(decodedStmt);
                    }
                    
                }
                //System.out.println("oracle table stmt: " + decodedStmt.getStmt());
                try{
                    boolean success = state.executeStatement(new SQLQueryAdapter(decodedStmt.getStmt(), errors));
                }catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
        state.historyIsUsed = true;
    }
}

