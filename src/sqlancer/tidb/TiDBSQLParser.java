package sqlancer.tidb;

import sqlancer.common.SQLParser;
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
    
    public static DecodedStmt parse(String queryString, String dbname) {

        String line = null;
        try {
            goCmdExec cmdExec = new goCmdExec();
            String scriptPath = "./src/sqlancer/go_parser/parse_a_stmt.sh";
            List<String> cmdInfo = new ArrayList<>();
            cmdInfo.add(scriptPath);
            cmdInfo.add(dbname);
            cmdInfo.add(queryString);
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
}
