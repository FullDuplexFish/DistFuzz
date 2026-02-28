package sqlancer.cockroachdb;

import sqlancer.common.SQLParser;
import sqlancer.common.DecodedStmt.stmtType;
import sqlancer.common.query.SQLQueryAdapter;

import sqlancer.GlobalState;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.common.AbstractMutator;
import sqlancer.common.DecodedStmt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;

public class CockroachDBMutator extends AbstractMutator{

    static CockroachDBGlobalState state;
    String sql;
    public CockroachDBMutator(CockroachDBGlobalState state, String sql) {
        super(state, sql);
        this.state = state;
        this.sql = sql;
    }
    
    public void setGlobalState(CockroachDBGlobalState state) {
        this.state = state;
    }
    private void removePartition() {
        if(sql.toLowerCase().contains("partition")) 
            sql = sql.substring(0, sql.toLowerCase().indexOf("partition"));
    }
    private String trimString(String str) {
        str = str.trim();
        if(str.endsWith(";")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
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

    private void generateRangePartition(List<String> inserts) {
        
        List<String> tables = extract_table_name_from_stmt(sql);
        
        //System.out.println("extracted table name " + tables);
        if(tables.size() == 0) {
            return;
        }
        state.getLogger().writeCurrent("mutating table " + tables.get(0));
        String table_name = tables.get(0);
        //System.out.println("here");
        String col = getRandomColumnStringUsingRegex();
        //System.out.println("extracted col name " + col);
        if(col == null) {//no int type column
            return;
        }
        state.getLogger().writeCurrent("select column " + col + " as partition key");
        
        List<Integer> bounds = new ArrayList<Integer>();
        int range = (int)Randomly.getNotCachedInteger(10, 10000);
        int cnt = (int)Randomly.getNotCachedInteger(10, 15);
        sql += " partition by range(" + col + ")(";
        int bound = range;
        for(int i = 0; i < cnt; i ++ ) {
            int next_bound = bound + range + (int)Randomly.getNotCachedInteger(0, 100);
            sql += "partition p" + String.valueOf(i) + " values from(" + String.valueOf(bound) + ") to (" + next_bound + "),";
            
            bounds.add(bound);
            bounds.add(bound + 1);
            bound = next_bound;
        }
        sql += "partition p" + String.valueOf(cnt) + " values from (" +  bound + ") to (maxvalue));";
        state.getLogger().writeCurrent("after mutating " + sql);
        

        
        int st = (int)state.getRandomly().getNotCachedInteger(-1000, 1000);
        for(int i = 0; i < 20; i ++ ) {
            bounds.add(i + st);
        }
        generateInsertsForBounds(col, bounds, inserts, table_name);
        return;
        
    }
    private void generateInsertsForBounds(String partitionKey, List<Integer> bounds, List<String> inserts, String table) {

        StringBuilder sb = new StringBuilder();

        sb.append("INSERT");

        sb.append(" INTO ");
        sb.append(table);
        sb.append(" VALUES ");
        List<CockroachDBColumn> columns = getCockroachDBColumns(extract_column_name_from_stmt(sql));
        insertColumns(sb, columns, partitionKey, bounds);
        inserts.add(sb.toString());
    }
    private void insertColumns(StringBuilder sb, List<CockroachDBColumn> columns, String partitionKey, List<Integer> bounds) {
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(state).setColumns(columns);
        for (int nrRows = 0; nrRows < bounds.size(); nrRows++) {
            if (nrRows != 0) {
                sb.append(", ");
            }
            sb.append("(");
            int i = 0;
            for (CockroachDBColumn c : columns) {
                if (i++ != 0) {
                    sb.append(", ");
                }
                //System.out.println("here!" + c.getName() + ' ' + partitionKey);
                if(!c.getName().equals(partitionKey)) {
                    sb.append(CockroachDBVisitor.asString(gen.generateConstant(c.getType())));
                }else{
                    
                    sb.append(bounds.get(nrRows));
                }
                
            }
            sb.append(")");
        }
    }
    private List<CockroachDBColumn> getCockroachDBColumns(List<String> cols) {
        List<CockroachDBColumn> res = new ArrayList<CockroachDBColumn>();
 
        for(int i = 0; i < cols.size(); i ++ ) {
            String type = extractColumnTypeFromStmt(sql, cols.get(i));
            state.getLogger().writeCurrent("try to get type of " + cols.get(i));
            state.getLogger().writeCurrent("parsing type string:" + type);
            if(type == null){
                state.getLogger().writeCurrent("type is null, sql is " + sql);
            }
            CockroachDBColumn cur = new CockroachDBColumn(cols.get(i), CockroachDBSchema.getColumnType(type), false, true);
            res.add(cur);
        }
        return res;
    }
    public String extractColumnTypeFromStmt(String str, String col) {
        // 正则表达式：匹配列的类型
        String regex = "(?<=" + col + "\\s).*?(?=[,\\)])"; // \b 表示单词边界，以避免匹配到类似 t1234abc 的表名
        
        Pattern pattern = Pattern.compile(regex,Pattern.DOTALL);
        System.out.println(pattern.toString());
        Matcher matcher = pattern.matcher(str);
        
        
        while (matcher.find()) {
            String cur =  matcher.group().trim();
            if(CockroachDBSchema.getColumnType(cur) != null) {
                return cur;
            }
        }
        return null;
    }

    private void appendPartition(List<String> inserts) {
        sql = trimString(sql);


        generateRangePartition(inserts);

    }
    public List<String> mutateDDL() {
        List<String> res = new ArrayList<String>();
        List<String> inserts = new ArrayList<String>();
        if(sql.toLowerCase().contains("create table")) {

            if(sql.toLowerCase().contains("partition")) {
                removePartition();
            }
            if(!sql.toLowerCase().contains("like")){
                appendPartition(inserts);
            }
        }
            //System.out.println("oracle table stmt: " + decodedStmt.getStmt());

        res.add(sql);
        res.addAll(inserts);
        return res;
    }

    public List<String> mutateSQL() {
        if(sql.toLowerCase().contains("create table")) {
            return mutateDDL();
        }else{
            return Arrays.asList(mutateDQL());
        }
    }
}

