package sqlancer.mysql.oracle;

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
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLMutator;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.gen.MySQLRandomQuerySynthesizer;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

public class MySQLPartitionOracle implements TestOracle<MySQLGlobalState> {

    private final MySQLGlobalState globalState;
    private List<String> history;
    private List<String> queries;
    private final ExpectedErrors errors;

    public MySQLPartitionOracle(MySQLGlobalState globalState) {
        this.globalState = globalState;
        this.history = globalState.getHistory();
        this.errors = globalState.getExpectedErrors();
        this.errors.add("only_full_group_by");
        this.errors.add("Duplicate entry");
        this.errors.add("Partition management on a not partitioned table is not possible");
        this.errors.add("Field in list of fields for partition function not found in table");
        this.errors.add("A BLOB field is not allowed in partition function");
        this.errors.add("of ORDER BY clause is not in SELECT list");
        this.errors.add("A UNIQUE INDEX must include all columns in the table's partitioning function");
        this.errors.add("Unknown column");
        this.errors.add("A PRIMARY KEY must include all columns in the table's partitioning function");
        this.errors.add("The storage engine for the table doesn't support descending indexes");
        this.errors.add("Table storage engine 'ndbcluster' does not support the create option");
        this.errors.add("can't be used in key specification with the used table type");
        this.errors.add("Can't create destination table for copying alter table");
        this.errors.add("Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys");
        this.errors.add("The storage engine for the table doesn't support native partitioning");
        this.errors.add("does not support the create option");
        this.errors.add("Specified key was too long");
        this.errors.add("Unknown storage engine 'NDBCLUSTER'");
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
 

    List<String> generateMultipleQueries() {

        List<String> res = new ArrayList<String>();
        for(int i = 0; i < 1; i ++ ) {
            //globalState.getLogger().writeCurrent(Integer.toString(i));
            try{
                String s = MySQLVisitor.asString(MySQLRandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1))
                    + ';';
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
            if(globalState.getRandomly().getBooleanWithSmallProbability()) {
                partition_table_oracle_simple();
            }
        }catch(Exception e) {
            e.printStackTrace();
        }
    }



    public void partition_table_oracle_simple() throws SQLException{

        for(String cur : queries) {
            if(globalState.getDbmsSpecificOptions().enableMutate && globalState.getRandomly().getBooleanWithSmallProbability()) {
                MySQLMutator mutator = new MySQLMutator(globalState, cur);
                cur = mutator.mutateDQL();
            }
            List<String> tables = extract_table_name_from_stmt(cur);
            try{
                for(String table : tables) {
                    String alter = "alter table " + table + " ";
                    alter = generateKeyPartition(alter, table);
                    globalState.executeStatement(new SQLQueryAdapter(alter, this.errors, false));
                }
                List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(cur, errors, globalState);
                
                for(String table : tables) {
                    globalState.executeStatement(new SQLQueryAdapter("alter table " + table + " remove partitioning", this.errors, false));
                }
                List<String> resultSet2 = ComparatorHelper.getResultSetFirstColumnAsString(cur, errors, globalState);
                ComparatorHelper.assumeResultSetsAreEqual(resultSet, resultSet2, cur, List.of(cur),
                globalState);
                globalState.getManager().incrementSelectQueryCount();
            }catch(Exception e) {
                e.printStackTrace();
            }
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
                        MySQLMutator mutator = new MySQLMutator(globalState, cur);
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
        String query = "";
        if(!globalState.getRandomly().getBoolean()) {
            query =  generateKeyPartition(str, name);
        }else{
            query = str;
        }
        
        
        try {
            boolean succ = globalState.executeStatement(new SQLQueryAdapter(query, this.errors, true));
            if(!succ) {
                query = query.substring(0, query.indexOf("partition"));
                succ = globalState.executeStatement(new SQLQueryAdapter(query, this.errors, true));
            }
            addTableToMap(succ, table_exists, name);
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

        this.errors.add("is not allowed in partition function");
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
    
    
    public String generateKeyPartition(String str, String table_name) {
        String col = globalState.getRandomColumnStrings(table_name);
        if(col == null || Randomly.getBooleanWithRatherLowProbability()) {
            col = "";
        }else{
            col = col.split(";")[0];
        }
        
        
        int pcnt = (int)Randomly.getNotCachedInteger(10, 100);
        str += " partition by key(";
        str += col;
        str += ") partitions ";
        str += String.valueOf(pcnt);
        return str;
    }

    public String replaceTableNameWithOracleName(List<String> tables, HashMap<String, Boolean> table_exists, String str) {
        /*if(flag == 4) {
            return "select 1";
        }*/
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
