package sqlancer.tidb;

import sqlancer.common.SQLParser;
import sqlancer.common.DecodedStmt.stmtType;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBCompositeDataType;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.visitor.TiDBVisitor;
import sqlancer.GlobalState;
import sqlancer.Randomly;
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
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;

public class TiDBMutator{

    static TiDBGlobalState state;
    DecodedStmt decodedStmt;
    public TiDBMutator(TiDBGlobalState state, String sql) {
        this.state = state;
        decodedStmt = TiDBSQLParser.parse(sql, state.getDatabaseName());
    }
    
    public void setGlobalState(TiDBGlobalState state) {
        this.state = state;
    }
    private void removePartition() {
        String str = decodedStmt.getStmt();
        decodedStmt.setStmt(str.substring(0, str.indexOf("partition")));
    }
    private String trimString(String str) {
        str = str.trim();
        if(str.endsWith(";")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }
    private void generateRangePartition(List<String> inserts) {
        if(decodedStmt.getTables().size() == 0) {
            return;
        }
        String str = decodedStmt.getStmt();
        String table_name = decodedStmt.getTables().get(0);
        String col = getRandomIntColumnStringUsingParser();

        if(col == null) {//no int type column
            return;
        }
        List<Integer> bounds = new ArrayList<Integer>();
        int range = (int)Randomly.getNotCachedInteger(10, 10000);
        int cnt = (int)Randomly.getNotCachedInteger(10, 15);
        str += " partition by range(" + col + ")(";
        int bound = range;
        for(int i = 0; i < cnt; i ++ ) {
            str += "partition p" + String.valueOf(i) + " values less than(" + String.valueOf(bound) + "),";
            bounds.add(bound);
            bounds.add(bound + 1);
            bound += range + (int)Randomly.getNotCachedInteger(0, 100);
        }
        str += "partition p" + String.valueOf(cnt) + " values less than maxvalue);";
        
        decodedStmt.setStmt(str);

        
        int st = (int)state.getRandomly().getNotCachedInteger(-1000, 1000);
        for(int i = 0; i < 20; i ++ ) {
            bounds.add(i + st);
        }
        generateInsertsForBounds(col, bounds, inserts);
        return;
        
    }
    private void generateInsertsForBounds(String partitionKey, List<Integer> bounds, List<String> inserts) {
        if(decodedStmt.getTables().size() == 0) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        String table = decodedStmt.getTables().get(0);
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
        List<TiDBColumn> columns = getColumnsFromDecodedStmt();
        insertColumns(sb, columns, partitionKey, bounds);
        inserts.add(sb.toString());
    }
    private void insertColumns(StringBuilder sb, List<TiDBColumn> columns, String partitionKey, List<Integer> bounds) {
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(state).setColumns(columns);
        for (int nrRows = 0; nrRows < bounds.size(); nrRows++) {
            if (nrRows != 0) {
                sb.append(", ");
            }
            sb.append("(");
            int i = 0;
            for (TiDBColumn c : columns) {
                if (i++ != 0) {
                    sb.append(", ");
                }
                if(c.getName() != partitionKey) {
                    sb.append(TiDBVisitor.asString(gen.generateConstant(c.getType().getPrimitiveDataType())));
                }else{
                    sb.append(bounds.get(nrRows));
                }
                
            }
            sb.append(")");
        }
    }
    private List<TiDBColumn> getColumnsFromDecodedStmt() {
        List<TiDBColumn> res = new ArrayList<TiDBColumn>();
        for(int i = 0; i < decodedStmt.getCols().size(); i ++ ) {
            TiDBColumn cur = new TiDBColumn(decodedStmt.getCols().get(i), TiDBSchema.getColumnType(decodedStmt.getColsType().get(i)), false, false, false);
            res.add(cur);
        }
        return res;
    }
    private String getRandomIntColumnStringUsingParser() {
        int i = 0;
        if(decodedStmt.getColsType() == null) {
            return null;
        }
        while(i < decodedStmt.getColsType().size()) {
            if(decodedStmt.getColsType().get(i).toLowerCase().contains("int")) {
                break;
            }
            i ++ ;
        }
        if(i < decodedStmt.getColsType().size() && decodedStmt.getColsType().get(i).toLowerCase().contains("int")) {
            return decodedStmt.getCols().get(i);
        }else{
            return null;
        }
        
    }
    private void generateHashPartition(List<String> inserts) {
        if(decodedStmt.getTables().size() == 0) {
            return;
        }
        String str = decodedStmt.getStmt();
        String table_name = decodedStmt.getTables().get(0);

        String col = getRandomIntColumnStringUsingParser();
        
        if(col == null) {
            return;
        }
        int pcnt = (int)Randomly.getNotCachedInteger(10, 15);
        str += " partition by hash(";
        str += col;
        str += ") partitions ";
        str += String.valueOf(pcnt);
        decodedStmt.setStmt(str);

        List<Integer> bounds = new ArrayList<Integer>();
        int st = (int)state.getRandomly().getNotCachedInteger(-1000, 1000);
        for(int i = 0; i < 20; i ++ ) {
            bounds.add(i + st);
        }
        generateInsertsForBounds(col, bounds, inserts);
        return;

    }
    private void appendPartition(List<String> inserts) {
        decodedStmt.setStmt(trimString(decodedStmt.getStmt()));

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
        decodedStmt.setStmt(decodedStmt.getStmt().toLowerCase());
        if(decodedStmt.getParseSuccess()) {
            if(decodedStmt.getStmtType() == DecodedStmt.stmtType.DDL) {
                if(decodedStmt.getStmt().toLowerCase().contains("partition")) {
                    removePartition();
                }
                appendPartition(inserts);
            }
            //System.out.println("oracle table stmt: " + decodedStmt.getStmt());
        }
        res.add(decodedStmt.getStmt());
        res.addAll(inserts);
        return res;
    }
    public String mutateDQL() {
        String sql = decodedStmt.getStmt();
        String col = getRandomIntColumnStringUsingParser();
        if(col == null) {
            return sql;
        }
        if(state.getRandomly().getBoolean()) {
            sql += " and ";
        }else{
            sql += " or ";
        }
        int leftBound = (int)state.getRandomly().getNotCachedInteger(-10000, 100000);
        int rightBound = (int)state.getRandomly().getNotCachedInteger(leftBound, 100000000);
        sql += " col >= " + Integer.toString(leftBound);
        if(state.getRandomly().getBoolean()) {
            sql += " and ";
        }else{
            sql += " or ";
        }
        sql += " col <= " + Integer.toString(rightBound);
        decodedStmt.setStmt(sql);
        return sql;

    }
    public List<String> mutateSQL() {
        if(decodedStmt.getStmtType() == stmtType.DDL) {
            return mutateDDL();
        }else{
            return Arrays.asList(mutateDQL());
        }
    }
}
