
package sqlancer.mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.mysql.MySQLSchema.MySQLColumn;

public class MySQLGlobalState extends SQLGlobalState<MySQLOptions, MySQLSchema> {
    public static ExpectedErrors errors = new ExpectedErrors();

    @Override
    protected MySQLSchema readSchema() throws SQLException {
        return MySQLSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MySQLOracleFactory.PQS);
    }

        public String getRandomIntColumnString(String table_name) {
            try {
                List<MySQLColumn> databaseColumns = MySQLSchema.getTableColumns(getConnection(), table_name, this.getDatabaseName());
                
                MySQLColumn col = this.getRandomly().fromList(databaseColumns);
                List<MySQLColumn> list = new ArrayList<MySQLColumn>();
                for(int i = 0; i < databaseColumns.size(); i ++ ) {
                    if(databaseColumns.get(i).getType().isInt()) {
                        list.add(databaseColumns.get(i));
                    }
                }
                if(list.isEmpty()) {
                    return null;
                }
                return this.getRandomly().fromList(list).getName();
            }catch(Exception e) {
                e.printStackTrace();
            }
            return null;
            
        }
    public String getRandomColumnStrings(String table_name) {
            try {
                List<MySQLColumn> databaseColumns = MySQLSchema.getTableColumns(getConnection(), table_name, this.getDatabaseName());
                String res = "";
                String tp = "";
                //getLogger().writeCurrent("querying the columns of " + table_name);
                //getLogger().writeCurrent("size is " + databaseColumns.size());
                if(databaseColumns.size() == 1) {
                    MySQLColumn col = this.getRandomly().fromList(databaseColumns);
                    res = col.getName();
                    if(col.getType().getPrimitiveDataType().isNumeric())
                        tp = "NUM";
                    else 
                        tp = "TEXT";
                }else{
                    int sz = (int)Randomly.getNotCachedInteger(1, databaseColumns.size());
                    for(int i = 0; i < sz; i ++ ) {
                        if(i != 0){ 
                            res += ",";
                            tp += ",";
                        }
                        int ran = (int)Randomly.getNotCachedInteger(0, databaseColumns.size());
                        res += databaseColumns.get(ran).getName();
                        //getLogger().writeCurrent("the type of column " + databaseColumns.get(ran).getName() + " is " + databaseColumns.get(ran).getType().getPrimitiveDataType());
                        if(databaseColumns.get(ran).getType().getPrimitiveDataType().isNumeric())
                            tp += "NUM";
                        else 
                            tp += "TEXT";
                        databaseColumns.remove(ran);
                    }
                }
                res += ";" + tp;
                return res;
                
                
            }catch(Exception e) {
                e.printStackTrace();
            }
            return null;
        }

}
