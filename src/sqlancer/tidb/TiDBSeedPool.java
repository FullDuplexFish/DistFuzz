package sqlancer.tidb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.File;
import java.util.*;
import sqlancer.Randomly;
import sqlancer.common.DecodedStmt;
import sqlancer.common.DecodedStmt.stmtType;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.AbstractSeedPool;
import sqlancer.GlobalState;

public class TiDBSeedPool extends AbstractSeedPool{

    private List<File> fileList;
	public static int inscnt = 0;
    private List<DecodedStmt> DDLSeedPool;
    private List<DecodedStmt> DMLSeedPool;
    private List<DecodedStmt> DQLSeedPool;
    private static TiDBSeedPool seedPool;
    private TiDBSeedPool(GlobalState state)
    {
        super(state);
        initPool("./src/sqlancer/tidb/TiDBSeeds");
    }
    public List<DecodedStmt> getDDLSeedPool() {
        return DDLSeedPool;
    }
    public List<DecodedStmt> getDMLSeedPool() {
        return DMLSeedPool;
    }
    public List<DecodedStmt> getDQLSeedPool() {
        return DQLSeedPool;
    }
    public DecodedStmt getDDLSeed() {
        DecodedStmt res = DDLSeedPool.get(0);
        DDLSeedPool.remove(0);
        res = refineSQL(res);
        return res;
    }
    public DecodedStmt refineSQL(DecodedStmt stmt) {
        
        return stmt;
    }

    public DecodedStmt getDMLSeed() {
        DecodedStmt res = DMLSeedPool.get(0);
        DMLSeedPool.remove(0);
        return res;
    }
    public DecodedStmt getDQLSeed() {
        DecodedStmt res = DQLSeedPool.get(0);
        DQLSeedPool.remove(0);
        return res;
    }
    public static synchronized TiDBSeedPool getInstance(GlobalState state) {
        if(inscnt == 0) {
            seedPool =  new TiDBSeedPool(state);
            inscnt ++ ;
            return seedPool;
        }
        return seedPool;
    }


    public void initPool(String path) {
		getFiles(path);

		try{

			for(File file: fileList){
				loadSeed(file);
			}

		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public List<File> getFileList(){
		return this.fileList;
	}
    public void addSeed(String str) { 
        DecodedStmt stmt = TiDBSQLParser.parse(str, super.getGlobalState().getDatabaseName());
        if(stmt.getStmtType() == stmtType.DDL) {
            addToDDLSeedPool(stmt);
        }else if(stmt.getStmtType() == stmtType.DML) {
            addToDMLSeedPool(stmt);
        }else if(stmt.getStmtType() == stmtType.DQL) {
            addToDQLSeedPool(stmt);
        }
    }
    public void addToDDLSeedPool(DecodedStmt stmt) {
        this.DDLSeedPool.add(stmt);
    }
    public void addToDMLSeedPool(DecodedStmt stmt) {
        this.DMLSeedPool.add(stmt);
    }
    public void addToDQLSeedPool(DecodedStmt stmt) {
        this.DQLSeedPool.add(stmt);
    }
	public void loadSeed(File file) {
		try {
			FileInputStream fis = new FileInputStream(file);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			//fw.write(file.getName());
            String str = "";
			while((line = br.readLine()) != null){
				str += line;
                if(str.endsWith(";")) {
                    this.addSeed(str);
                    str = "";
                }
			}

			//fw.close();
		}catch(Exception e){
			e.printStackTrace();
		}

	}

	private void getFiles(String filePath){
		File root = new File(filePath);
		  File[] files = root.listFiles();
		  if(files == null) return;
		  for(File file:files){     
		   if(file.isDirectory()){
			/*
			 * 递归调用
			 */
			getFiles(file.getAbsolutePath());
		   }else{
				if((file.getName().endsWith(".test"))) {//seeds are end with .test or .result
					this.fileList.add(file);
				}
		   }     
		  }
	}

}
