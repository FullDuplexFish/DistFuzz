package sqlancer.tidb;

import sqlancer.common.SQLParser;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.GlobalState;
import sqlancer.common.DecodedStmt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

public class TiDBSQLParser implements SQLParser{

    DecodedStmt stmt;
    public static TiDBGlobalState state;
    
    public static void setGlobalState(TiDBGlobalState s) {
        state = s;
    }
    
    public static DecodedStmt parse(String queryString, String dbname) {

        String line = null;
        try {
            goCmdExec cmdExec = new goCmdExec();
            String scriptPath = "./src/sqlancer/go_parser/parse_a_stmt.sh";
            List<String> cmdInfo = new ArrayList<>();
            cmdInfo.add(scriptPath);
            cmdInfo.add(dbname);
            cmdInfo.add(queryString);
            //System.out.println(scriptPath + " " + dbname + " " + queryString);
            line = cmdExec.executeLinuxCmd(cmdInfo);
            
            // File file = new File("./tmp/stmt_parse_res_"+ uid);//seeds after refining are stored in seed_refine_res
            // FileInputStream fis = new FileInputStream(file);
            // BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            

            // line = br.readLine();
            
        }catch(Exception e) {
            e.printStackTrace();
        }

        return JSON.parseObject(line, DecodedStmt.class);
    } 
    public static String getTablesAndColumns() {
        List<String> tables = new ArrayList<String>();
        List<List<String> > columns = new ArrayList<List<String>> ();
        List<TiDBTable> tidb_tables = state.getSchema().getDatabaseTables();
        for(TiDBTable table: tidb_tables) {
            tables.add("\"" + table.getName() + "\"");
            List<String> cols = new ArrayList<String>();
            try{
                List<TiDBColumn> tidb_cols = TiDBSchema.getTableColumns(state.getConnection(), table.getName());
                for(TiDBColumn column: tidb_cols) {
                    cols.add("\"" + column.getName() + "\"");
                }
                columns.add(cols);
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
        String res = "{\"tables\":[";
        res += String.join(",", tables);
        res += "],\"columns\":[";
        for(int i = 0; i < columns.size(); i ++ ) {
            String cur = "[" + String.join(",", columns.get(i)) + "]";
            if(i > 0) {
                res += ",";
            }
            res += cur;
        }
        res += "]}";
        return res;
    }
    public static String refineSQL(String sql) {
        String line = null;
        try {
            goCmdExec cmdExec = new goCmdExec();
            String scriptPath = "./src/sqlancer/go_parser/refine_seed.sh";
            List<String> cmdInfo = new ArrayList<>();
            String args = getTablesAndColumns();
            cmdInfo.add(scriptPath);
            cmdInfo.add(args);
            cmdInfo.add(sql);
            System.out.println(scriptPath + " " + args + " " + sql);
            line = cmdExec.executeLinuxCmd(cmdInfo);
            
            // File file = new File("./tmp/stmt_parse_res_"+ uid);//seeds after refining are stored in seed_refine_res
            // FileInputStream fis = new FileInputStream(file);
            // BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            

            // line = br.readLine();
            
        }catch(Exception e) {
            e.printStackTrace();
        }

        return line.toLowerCase();
    }
}
