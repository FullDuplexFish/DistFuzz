package sqlancer.common;

import java.util.List;

/*
 * This class stores information of stmts decoded to json by parser.
 */
public class DecodedStmt {
    String stmt;
    public enum stmtType {DDL, DML, DQL, OTHER};
    stmtType type;
    List<String> tables;
    List<String> cols;
    List<String> colsType;
    boolean parseSuccess;

    public String getStmt() {
        return stmt;
    }
    public void setStmt(String stmt) {
        this.stmt = stmt;
    }
    public stmtType getStmtType() {
        return type;
    }
    public void setStmtType(stmtType type) {
        this.type = type;
    }
    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }
    public List<String> getCols() {
        return cols;
    }
    public void setCols(List<String> cols) {
        this.cols = cols;
    }
    public List<String> getColsType() {
        return colsType;
    }
    public void setColsType(List<String> colsType) {
        this.colsType = colsType;
    }
    public boolean getParseSuccess() {
        return parseSuccess;
    }
    public void setParseSuccess(boolean parseSuccess) {
        this.parseSuccess = parseSuccess;
    }

}
