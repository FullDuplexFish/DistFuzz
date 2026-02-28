package sqlancer.mysql;

import sqlancer.common.SQLParser;
import sqlancer.common.DecodedStmt.stmtType;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.gen.MySQLExpressionGenerator;
import sqlancer.GlobalState;
import sqlancer.Randomly;
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

public class MySQLMutator extends AbstractMutator{

    static MySQLGlobalState state;
    String sql;
    public MySQLMutator(MySQLGlobalState state, String sql) {
        super(state, sql);
        this.state = state;
        this.sql = sql;
    }
    
    public void setGlobalState(MySQLGlobalState state) {
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
        String table_name = tables.get(0);
        //System.out.println("here");
        String col = getRandomColumnStringUsingRegex();
        //System.out.println("extracted col name " + col);
        if(col == null) {//no int type column
            return;
        }
        
        List<Integer> bounds = new ArrayList<Integer>();
        int range = (int)Randomly.getNotCachedInteger(10, 10000);
        int cnt = (int)Randomly.getNotCachedInteger(10, 15);
        sql += " partition by range(" + col + ")(";
        int bound = range;
        for(int i = 0; i < cnt; i ++ ) {
            sql += "partition p" + String.valueOf(i) + " values less than(" + String.valueOf(bound) + "),";
            bounds.add(bound);
            bounds.add(bound + 1);
            bound += range + (int)Randomly.getNotCachedInteger(0, 100);
        }
        sql += "partition p" + String.valueOf(cnt) + " values less than maxvalue);";
        

        
        int st = (int)state.getRandomly().getNotCachedInteger(-1000, 1000);
        for(int i = 0; i < 20; i ++ ) {
            bounds.add(i + st);
        }
        generateInsertsForBounds(col, bounds, inserts, table_name);
        return;
        
    }
    private void generateInsertsForBounds(String partitionKey, List<Integer> bounds, List<String> inserts, String table) {

        StringBuilder sb = new StringBuilder();
        boolean isInsert = Randomly.getBoolean();
        if (isInsert) {
            sb.append("INSERT");
        } else {
            sb.append("REPLACE");
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"));
        }
        if (isInsert && Randomly.getBoolean()) {
            sb.append(" IGNORE ");
        }
        sb.append(" INTO ");
        sb.append(table);
        sb.append(" VALUES ");
        List<MySQLColumn> columns = getMySQLColumns(extract_column_name_from_stmt(sql));
        insertColumns(sb, columns, partitionKey, bounds);
        inserts.add(sb.toString());
    }
    private void insertColumns(StringBuilder sb, List<MySQLColumn> columns, String partitionKey, List<Integer> bounds) {
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(state).setColumns(columns);
        for (int nrRows = 0; nrRows < bounds.size(); nrRows++) {
            if (nrRows != 0) {
                sb.append(", ");
            }
            sb.append("(");
            int i = 0;
            for (MySQLColumn c : columns) {
                if (i++ != 0) {
                    sb.append(", ");
                }
                //System.out.println("here!" + c.getName() + ' ' + partitionKey);
                if(!c.getName().equals(partitionKey)) {
                    sb.append(MySQLVisitor.asString(gen.generateConstant(c.getType().getPrimitiveDataType())));
                }else{
                    
                    sb.append(bounds.get(nrRows));
                }
                
            }
            sb.append(")");
        }
    }
    private List<MySQLColumn> getMySQLColumns(List<String> cols) {
        List<MySQLColumn> res = new ArrayList<MySQLColumn>();
        for(int i = 0; i < cols.size(); i ++ ) {
            String type = extractColumnTypeFromStmt(sql, cols.get(i));
            MySQLColumn cur = new MySQLColumn(cols.get(i), MySQLSchema.getColumnType(type), false, 10);
            res.add(cur);
        }
        return res;
    }
    private String extractColumnTypeFromStmt(String str, String col) {
        // 正则表达式：匹配列的类型
        String regex = "(?<=" + col + "\\s).*?(?=[,\\)])"; // \b 表示单词边界，以避免匹配到类似 t1234abc 的表名
        
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        
        List<String> tableNames = new ArrayList<>();
        
        if (matcher.find()) {
            return matcher.group().trim();
        }
        return null;
    }
    private void generateHashPartition(List<String> inserts) {

        List<String> tables = extract_table_name_from_stmt(sql);
        if(tables.size() == 0) {
            return;
        }
        String table_name = tables.get(0);
        String col = getRandomColumnStringUsingRegex();
        
        if(col == null) {
            return;
        }
        int pcnt = (int)Randomly.getNotCachedInteger(10, 15);
        sql += " partition by hash(";
        sql += col;
        sql += ") partitions ";
        sql += String.valueOf(pcnt);

        List<Integer> bounds = new ArrayList<Integer>();
        int st = (int)state.getRandomly().getNotCachedInteger(-1000, 1000);
        for(int i = 0; i < 20; i ++ ) {
            bounds.add(i + st);
        }
        generateInsertsForBounds(col, bounds, inserts, table_name);
        return;

    }
    private void appendPartition(List<String> inserts) {
        sql = trimString(sql);

        switch((int)state.getRandomly().getNotCachedInteger(0, 2)) {
            case(0):
                generateRangePartition(inserts);
                break;
            default:
                generateHashPartition(inserts);
            
        }
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

