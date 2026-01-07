package sqlancer.mysql.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;
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

public class MySQLOptimizationOracle implements TestOracle<MySQLGlobalState> {

    private final MySQLGlobalState globalState;
    private List<List<String> > queries;
    private final ExpectedErrors errors;

    public MySQLOptimizationOracle(MySQLGlobalState globalState) {
        this.globalState = globalState;
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
 

    List<List<String> > generateMultipleQueries() {
        List<String> arr = new ArrayList<String>();
        List<List<String> > res = new ArrayList<List<String>>();
        for(int i = 0; i < 30; i ++ ) {
            //globalState.getLogger().writeCurrent(Integer.toString(i));
            try{
                String s = MySQLVisitor.asString(MySQLRandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1))
                    + ';';
                //globalState.getLogger().writeCurrent(s);
                arr.add(s);
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
        res.add(arr);
        //globalState.getLogger().writeCurrent("finish generating queries with res size: " + arr.size());
        return res;
    }
    @Override
    public void check() throws Exception {
        queries = generateMultipleQueries();
        try{
            engine_oracle(); //this doesn't work, sad
            optimization_oracle();//I hope this work, really
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    public void close_optimization() throws SQLException {
        String ban_op = "SET session optimizer_switch = 'index_merge=off,index_merge_union=off,index_merge_sort_union=off,index_merge_intersection=off,engine_condition_pushdown=off,index_condition_pushdown=off,mrr=off,mrr_cost_based=off,block_nested_loop=off,batched_key_access=off,materialization=off,semijoin=off,loosescan=off,firstmatch=off,duplicateweedout=off,subquery_materialization_cost_based=off,use_index_extensions=off,condition_fanout_filter=off,derived_merge=off,use_invisible_indexes=off,skip_scan=off,hash_join=off,subquery_to_derived=off,prefer_ordering_index=off,derived_condition_pushdown=off,hash_set_operations=off';";
        //if in td sql, use this:
        //String ban_op = "SET session optimizer_switch = 'index_merge=off,index_merge_union=off,index_merge_sort_union=off,index_merge_intersection=off,engine_condition_pushdown=off,index_condition_pushdown=off,mrr=off,mrr_cost_based=off,block_nested_loop=off,batched_key_access=off,materialization=off,semijoin=off,loosescan=off,firstmatch=off,duplicateweedout=off,subquery_materialization_cost_based=off,use_index_extensions=off,condition_fanout_filter=off,derived_merge=off';";
        try{
            globalState.executeStatement(new SQLQueryAdapter(ban_op, this.errors, false));
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public void open_optimization() throws SQLException {
        String ban_op = "SET session optimizer_switch = 'index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=on,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=on,skip_scan=on,hash_join=on,subquery_to_derived=on,prefer_ordering_index=on,derived_condition_pushdown=on,hash_set_operations=on';";
        //if in td sql, use this:
        //String ban_op = "SET session optimizer_switch = 'index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=on,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on';";
        try{
            globalState.executeStatement(new SQLQueryAdapter(ban_op, this.errors, false));
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public void optimization_oracle() throws SQLException {
        for(List<String> strs : queries) {
            for(String cur : strs) {
                if(globalState.getRandomly().getBoolean()) {
                    MySQLMutator mutator = new MySQLMutator(globalState, cur);
                    cur = mutator.mutateDQL();
                }
                List<String> tables = extract_table_name_from_stmt(cur);
                try{
                    for(String table : tables) {
                        if(Randomly.getBoolean()) {
                            String alter = "alter table " + table + " engine=NDBCLUSTER";
                            globalState.executeStatement(new SQLQueryAdapter(alter, this.errors, false));
                        }
                    }
                    String ban_op = "set session optimizer_prune_level=0";
                    globalState.executeStatement(new SQLQueryAdapter(ban_op, this.errors, false));
                    ban_op = "set session optimizer_search_depth=0";
                    globalState.executeStatement(new SQLQueryAdapter(ban_op, this.errors, false));
                    //ban_op = "SET session optimizer_switch = 'index_merge=off,index_merge_union=off,index_merge_sort_union=off,index_merge_intersection=off,engine_condition_pushdown=off,index_condition_pushdown=off,mrr=off,mrr_cost_based=off,block_nested_loop=off,batched_key_access=off,materialization=off,semijoin=off,loosescan=off,firstmatch=off,duplicateweedout=off,subquery_materialization_cost_based=off,use_index_extensions=off,condition_fanout_filter=off,derived_merge=off,use_invisible_indexes=off,skip_scan=off,hash_join=off,subquery_to_derived=off,prefer_ordering_index=off,derived_condition_pushdown=off,hash_set_operations=off';";
                    //globalState.executeStatement(new SQLQueryAdapter(ban_op, this.errors, false));
                    close_optimization();
                    List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(cur, errors, globalState);
                    
                    ban_op = "set session optimizer_prune_level=1";
                    globalState.executeStatement(new SQLQueryAdapter(ban_op, this.errors, false));
                    ban_op = "set session optimizer_search_depth=10";
                    globalState.executeStatement(new SQLQueryAdapter(ban_op, this.errors, false));
                    //ban_op = "SET session optimizer_switch = 'index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=on,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=on,skip_scan=on,hash_join=on,subquery_to_derived=on,prefer_ordering_index=on,derived_condition_pushdown=on,hash_set_operations=on';";
                    //globalState.executeStatement(new SQLQueryAdapter(ban_op, this.errors, false));
                    open_optimization();
                    List<String> resultSet2 = ComparatorHelper.getResultSetFirstColumnAsString(cur, errors, globalState);
                    ComparatorHelper.assumeResultSetsAreEqual(resultSet, resultSet2, cur, List.of(cur),
                    globalState);
                    globalState.getManager().incrementSelectQueryCount();
                }catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public void engine_oracle() throws SQLException {
        for(List<String> strs : queries) {
            for(String cur : strs) {

                List<String> tables = extract_table_name_from_stmt(cur);
                try{
                    for(String table : tables) {
                        String alter = "alter table " + table + " engine=NDBCLUSTER";
                        globalState.executeStatement(new SQLQueryAdapter(alter, this.errors, false));
                    }
                    List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(cur, errors, globalState);
                    
                    for(String table : tables) {
                        if(Randomly.getBoolean())
                            globalState.executeStatement(new SQLQueryAdapter("alter table " + table + " engine=innodb", this.errors, false));
                        else
                            globalState.executeStatement(new SQLQueryAdapter("alter table " + table + " engine=MyISAM", this.errors, false));
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

    public String replaceTableNameWithOracleName(List<String> tables, HashMap<String, Boolean> table_exists, String str, int flag) {
        /*if(flag == 4) {
            return "select 1";
        }*/
        for(String name : tables) {
            String new_name = name + "_oracle" + String.valueOf(flag);
            /*if(table_exists.containsKey(new_name) && table_exists.get(new_name) == true) {
                str = state.replaceStmtTableName(str, name, new_name);
            }*/
            str = globalState.replaceStmtTableName(str, name, new_name);//rather than checking whether table exists, just ignore all 'not exists' is a better option
        }
        return str;
    }

 
    public void compareResult(String q1, String q2) {
        try {
            List<String> result1 = ComparatorHelper.getResultSetFirstColumnAsString(q1, errors, globalState);
            List<String> result2 = ComparatorHelper.getResultSetFirstColumnAsString(q2, errors, globalState);
            ComparatorHelper.assumeResultSetsAreEqual(result1, result2, q1, List.of(q2), globalState);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

}
