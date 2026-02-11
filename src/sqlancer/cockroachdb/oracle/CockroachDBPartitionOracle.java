package sqlancer.cockroachdb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlElementDecl.GLOBAL;

import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.cockroachdb.*;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBJoin.JoinType;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.cockroachdb.gen.CockroachDBRandomQuerySynthesizer;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

public class CockroachDBPartitionOracle implements TestOracle<CockroachDBGlobalState> {

    private final CockroachDBGlobalState globalState;
    private List<String> history;
    private List<String> queries;
    private final ExpectedErrors errors;

    public CockroachDBPartitionOracle(CockroachDBGlobalState globalState) {
        this.globalState = globalState;
        this.history = globalState.getHistory();
        this.errors = globalState.getExpectedErrors();
    }
    List<String> extract_table_name_from_stmt(String sql) {
        // 正则表达式：匹配以 t 开头，后面跟数字的表名
        String regex = "\\bt\\d+\\b"; // \b 表示单词边界，以避免匹配到类似 t1234abc 的表名
        
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(sql);
        
        List<String> tableNames = new ArrayList<>();
        
        while (matcher.find()) {
            tableNames.add(matcher.group());
        }
        tableNames = tableNames.stream()
                               .distinct()
                               .collect(Collectors.toList());
        return tableNames;
    }
 
        private String generateSelect() {
        CockroachDBSelect select = new CockroachDBSelect();
        CockroachDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables(2);
        List<CockroachDBExpression> tableList = CockroachDBCommon.getTableReferences(
                tables.getTables().stream().map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList()));
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState).setColumns(tables.getColumns());
        List<CockroachDBExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream()
                .map(c -> new CockroachDBColumnReference(c)).collect(Collectors.toList()));
        
        select.setFetchColumns(fetchColumns);
        select.setFromList(tableList);
        select.setDistinct(Randomly.getBoolean());
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(fetchColumns);
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
            }
        }

        // Set the join.
        List<CockroachDBExpression> joinExpressions = getJoins(tableList, globalState);
        select.setJoinList(joinExpressions);

        // Get the result of the first query
        String queryString1 = CockroachDBVisitor.asString(select);

        return queryString1;

    }
    private List<CockroachDBExpression> getJoins(List<CockroachDBExpression> tableList,
        CockroachDBGlobalState globalState) throws AssertionError {
        List<CockroachDBExpression> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getPercentage() < 0.8) {
            CockroachDBTableReference leftTable = (CockroachDBTableReference) tableList.remove(0);
            CockroachDBTableReference rightTable = (CockroachDBTableReference) tableList.remove(0);
            List<CockroachDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            CockroachDBExpressionGenerator joinGen = new CockroachDBExpressionGenerator(globalState)
                    .setColumns(columns);
            joinExpressions.add(CockroachDBJoin.createJoin(leftTable, rightTable,
                    CockroachDBJoin.JoinType.getRandomExcept(JoinType.NATURAL),
                    joinGen.generateExpression(CockroachDBDataType.BOOL.get())));
        }
        return joinExpressions;
    }
    public List<String> generateMultipleQueries() {

        List<String> res = new ArrayList<String>();
        for(int i = 0; i < 30; i ++ ) {
            //globalState.getLogger().writeCurrent(Integer.toString(i));
            try{
                String s = generateSelect();
                //globalState.getLogger().writeCurrent(s);
                res.add(s);
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
        //globalState.getLogger().writeCurrent("finish generating queries with res size: " + arr.size());
        return res;
    }
    @Override
    public void check() throws Exception {
        System.out.println("executing partition oracle");
        this.globalState.getLogger().writeCurrent("executing oracle and history size is :" + history.size());
        queries = generateMultipleQueries();
        this.globalState.getLogger().writeCurrent("executing oracle and query size is :" + queries.size());
        try{
            partition_table_oracle();//this oracle has too many corner cases
 
        }catch(Exception e) {
            e.printStackTrace();
        }
    }




    public void partition_table_oracle() throws SQLException {
        try {
            List<String> tables = getTables();
            HashMap<String, Boolean> table_exists = new HashMap<String, Boolean>();
            if(!globalState.historyIsUsed) {
                for(int i = 0; i < history.size(); i ++ ) {
                
                    generate_partition_oracle_stmt(tables, table_exists, history.get(i));
                }
                globalState.historyIsUsed = true;
            }
            
            
            for(String cur : queries) {
                    if(globalState.getRandomly().getBooleanWithSmallProbability()) {
                        CockroachDBMutator mutator = new CockroachDBMutator(globalState, cur);
                        cur = mutator.mutateDQL();
                    }
                    String q1 = cur;
                    String q2 = q1;
                    for(String table: tables) {
                        if(q2.contains(table)) {
                            //state.getLogger().writeCurrent("checking whether " + table + " in map");
                            String newName = table + "_oracle";
                            if(table_exists.containsKey(newName) && table_exists.get(newName) == true) {
                                q2 = globalState.replaceStmtTableName(q2, table, newName);
                            }
                        }
                    }
                    compareResult(q1, q2);
            }

        }catch(Exception e) {
            e.printStackTrace();

        }
    }
    
    public List<String> getTables() throws Exception {
        SQLQueryAdapter q = new SQLQueryAdapter("show tables");
        List<String> res = new ArrayList<String>();
        try (SQLancerResultSet rs = q.executeAndGet(this.globalState)) {
            if (rs != null) {
                
                while (rs.next()) {
                    String name = rs.getString(1);
                    if(!name.contains("oracle"))
                        res.add(name);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } 
        return res;
    }
    private String trimString(String str) {
        str = str.trim();
        if(str.startsWith("`")) {
            str = str.substring(1, str.length());
        }
        if(str.endsWith("`")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }
    private void addHashPartition(String name) {
        String stmt = "create index on " + name + "(";
        String col = globalState.getRandomColumnStrings(name);
        stmt += col;
        stmt += ") using hash";
        try {
            globalState.executeStatement(new SQLQueryAdapter(stmt, this.errors, true));
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    private void process_create_table(List<String> tables, HashMap<String, Boolean> table_exists, String str) {
        if(str.toLowerCase().contains("partition")) {
            str = str.substring(0, str.indexOf("partition"));//if ddl has partition definition, remove it
        }
        String name = extract_table_name_from_stmt(str).get(0);
        name = trimString(name);
        str = str.replaceFirst(name, name + "_oracle");
        str = str.trim();
        if(str.endsWith(";")) {
            str = str.substring(0, str.length() - 1);
        }
        String drop_ex = "drop table if exists " + name + "_oracle;";
        try {
            globalState.executeStatement(new SQLQueryAdapter(drop_ex, this.errors, true));
        }catch(Exception e) {
            e.printStackTrace();
        }
        String query = str;
        
        
        try {
            boolean succ = globalState.executeStatement(new SQLQueryAdapter(query, this.errors, true));
            if(!succ) {
                query = query.substring(0, query.indexOf("partition"));
                succ = globalState.executeStatement(new SQLQueryAdapter(query, this.errors, true));
            }
            addTableToMap(succ, table_exists, name);

            addHashPartition(name);
        }catch(Exception e) {
            e.printStackTrace();
        }
            
        return;
        
    }
    private void process_create_table_like(List<String> tables, HashMap<String, Boolean> table_exists, String str) {

        List<String> names = extract_table_name_from_stmt(str);
        str = replaceTableNameWithOracleName(tables, table_exists, str);
        if(str.endsWith(";")) {
            str = str.substring(0, str.length() - 1);
        }
        String drop_ex = "drop table if exists " + names.get(0) + "_oracle;";
        try {
            globalState.executeStatement(new SQLQueryAdapter(drop_ex, this.errors, true));
        }catch(Exception e) {
            e.printStackTrace();
        }

        try {
            boolean succ = globalState.executeStatement(new SQLQueryAdapter(str, this.errors, true));
            addTableToMap(succ, table_exists, names.get(0));
        }catch(Exception e) {
            e.printStackTrace();
        }
            
        return;
        
    }
    private void generate_partition_oracle_stmt(List<String> tables, HashMap<String, Boolean> table_exists, String str) throws Exception{

  
        str = str.toLowerCase();
        if(str.toLowerCase().contains("create table") && !str.toLowerCase().contains("like")) {
            process_create_table(tables, table_exists, str);
            
        }else if(str.toLowerCase().contains("create table") && str.toLowerCase().contains("like")) {//create table like
            process_create_table_like(tables, table_exists, str);
        }
        else if(str.toLowerCase().contains("create")) {//if str is create temporary table, view, or index, then ignore
            globalState.executeStatement(new SQLQueryAdapter(str, this.errors, true));
        }else{

            String query = "";
            query = replaceTableNameWithOracleName(tables, table_exists, str);
            boolean succ = globalState.executeStatement(new SQLQueryAdapter(query, this.errors, true));
        }
        //addTableToMap(succ, table_exists, name, flag);
    }
    private void addTableToMap(boolean succ, HashMap<String, Boolean> table_exists, String name) {
        if(succ) {
            table_exists.put(name + "_oracle" , true);
        }
        else {
            table_exists.put(name + "_oracle", false);
        }
    }
    
    


    public String replaceTableNameWithOracleName(List<String> tables, HashMap<String, Boolean> table_exists, String str) {

        for(String name : tables) {
            String new_name = name + "_oracle";
            if(table_exists.containsKey(new_name) && table_exists.get(new_name) == true) {
                str = globalState.replaceStmtTableName(str, name, new_name);
            }
            //str = globalState.replaceStmtTableName(str, name, new_name);//rather than checking whether table exists, just ignore all 'not exists' is a better option
        }
        return str;
    }

    public String getTableNameFromCreate(String str) {
        int i = str.toLowerCase().indexOf("create table ") + 13;
        //if(str.contains("create table if exists"))
        String res = "";
        for(; i < str.length(); i ++ ) {
            if(str.charAt(i) == ' ' || str.charAt(i) == '(') {
                break;
            }
            res += str.charAt(i);
        }
        return res;
    }

    public void compareResult(String q1, String q2) {
        try {
            List<String> result1 = ComparatorHelper.getResultSetFirstColumnAsString(q1, errors, globalState);
            List<String> result2 = ComparatorHelper.getResultSetFirstColumnAsString(q2, errors, globalState);
            ComparatorHelper.assumeResultSetsAreEqual(result1, result2, q1, List.of(q2), globalState);
            globalState.getManager().incrementSelectQueryCount();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

}
