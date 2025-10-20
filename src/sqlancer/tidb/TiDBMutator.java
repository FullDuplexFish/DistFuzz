package sqlancer.tidb;

import sqlancer.common.SQLParser;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.GlobalState;
import sqlancer.Randomly;
import sqlancer.common.DecodedStmt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.alibaba.fastjson.JSON;

public class TiDBMutator{

    static TiDBGlobalState state;
    
    public void setGlobalState(TiDBGlobalState state) {
        this.state = state;
    }
    private void removePartition(DecodedStmt stmt) {
        String str = stmt.getStmt();
        stmt.setStmt(str.substring(0, str.indexOf("partition")));
    }
    private String trimString(String str) {
        str = str.trim();
        if(str.endsWith(";")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }
    private void generateRangePartition(DecodedStmt stmt) {
        if(stmt.getTables().size() == 0) {
            return;
        }
        String str = stmt.getStmt();
        String table_name = stmt.getTables().get(0);
        String col = stmt.getCols().get(new Random().nextInt(stmt.getCols().size()));

        if(col == null) {//no int type column
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
    private void appendPartition(DecodedStmt stmt) {
        stmt.setStmt(trimString(stmt.getStmt()));

        switch((int)state.getRandomly().getNotCachedInteger(0, 2)) {
            case(0):
                generateRangePartition(stmt);
                break;
            case(1):
                generateHashPartition(stmt);
                break;
            
        }
    }
    public String mutateDDL(String sql) {
        DecodedStmt decodedStmt = TiDBSQLParser.parse(sql, state.getDatabaseName());
        decodedStmt.setStmt(decodedStmt.getStmt().toLowerCase());
        if(decodedStmt.getParseSuccess()) {
            if(decodedStmt.getStmtType() == DecodedStmt.stmtType.DDL) {
                if(decodedStmt.getStmt().toLowerCase().contains("partition")) {
                    removePartition(decodedStmt);
                }
                appendPartition(decodedStmt);
            }
            //System.out.println("oracle table stmt: " + decodedStmt.getStmt());
        }
        return decodedStmt.getStmt();
    }
    // public String mutateDQL(String sql) {

    // }
}
