package sqlancer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.tidb.TiDBSchema;
import sqlancer.tidb.TiDBSchema.TiDBColumn;

public abstract class GlobalState<O extends DBMSSpecificOptions<?>, S extends AbstractSchema<?, ?>, C extends SQLancerDBConnection> {

    protected C databaseConnection;
    private Randomly r;
    private MainOptions options;
    private O dbmsSpecificOptions;
    private S schema;
    private Main.StateLogger logger;
    private StateToReproduce state;
    private Main.QueryManager<C> manager;
    private String databaseName;
    private List<String> history;
    public boolean historyIsUsed = false;
    private ExpectedErrors errors = new ExpectedErrors();
    public ExpectedErrors getExpectedErrors() {
        return errors;
    }

    public List<String> getHistory() {
        return history;
    }
    public void clearHistory() {
        history.clear();
    }
    public void initHistory() {
        history = new ArrayList<String>();
    }
    public void insertIntoHistory(String stmt) {
        history.add(stmt);
    }

    public void setConnection(C con) {
        this.databaseConnection = con;
    }

    public C getConnection() {
        return databaseConnection;
    }

    @SuppressWarnings("unchecked")
    public void setDbmsSpecificOptions(Object dbmsSpecificOptions) {
        this.dbmsSpecificOptions = (O) dbmsSpecificOptions;
    }

    public O getDbmsSpecificOptions() {
        return dbmsSpecificOptions;
    }

    public void setRandomly(Randomly r) {
        this.r = r;
    }

    public Randomly getRandomly() {
        return r;
    }

    public MainOptions getOptions() {
        return options;
    }

    public void setMainOptions(MainOptions options) {
        this.options = options;
    }

    public void setStateLogger(Main.StateLogger logger) {
        this.logger = logger;
    }

    public Main.StateLogger getLogger() {
        return logger;
    }

    public void setState(StateToReproduce state) {
        this.state = state;
    }

    public StateToReproduce getState() {
        return state;
    }

    public Main.QueryManager<C> getManager() {
        return manager;
    }

    public void setManager(Main.QueryManager<C> manager) {
        this.manager = manager;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    private ExecutionTimer executePrologue(Query<?> q) throws Exception {
        boolean logExecutionTime = getOptions().logExecutionTime();
        ExecutionTimer timer = null;
        if (logExecutionTime) {
            timer = new ExecutionTimer().start();
        }
        if (getOptions().printAllStatements()) {
            System.out.println(q.getLogString());
        }
        if (getOptions().logEachSelect()) {
            if (logExecutionTime) {
                getLogger().writeCurrentNoLineBreak(q.getLogString());
            } else {
                getLogger().writeCurrent(q.getLogString());
            }
        }
        return timer;
    }
    public String replaceStmtTableName(String origin_str, String oldName, String newName) {
        //String newString = origin_str.replaceAll(old_name + "(?![0-9]+)", new_name);
        // 安全转义表名
        String escapedOldName = Pattern.quote(oldName);
        
        // 构建正则表达式
        // 注意字符类中的转义：
        // - 需要转义为 \\-
        // ] 需要转义为 \\]
        // ^ 如果不是开头，不需要特殊转义
        
        String regex = "(?xi)                       # 忽略大小写和注释模式\n" +
                      "(?<=                        # 正向回顾\n" +
                      "    ^                       # 字符串开始\n" +
                      "    |                       # 或\n" +
                      "    [\\s,.()=                # 空白、逗号、点、左括号、等号\n" +
                      "     \\-+*/%&|^<>!~]        # 各种运算符\n" +
                      ")                           # 回顾结束\n" +
                      "\\b" + escapedOldName + "\\b # 精确匹配表名\n" +
                      "(?=                         # 正向预测\n" +
                      "    [\\s.,();=               # 空白、点、逗号、右括号、分号、等号\n" +
                      "     \\-+*/%&|^<>!~]        # 各种运算符\n" +
                      "    |                       # 或\n" +
                      "    $                       # 字符串结束\n" +
                      ")";
        
        // 使用编译模式，性能更好
        Pattern pattern = Pattern.compile(regex, Pattern.COMMENTS);
        Matcher matcher = pattern.matcher(origin_str);
        
        return matcher.replaceAll(newName);
    }

    protected abstract void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception;

    public boolean executeStatement(Query<C> q, String... fills) throws Exception {
        ExecutionTimer timer = executePrologue(q);
        boolean success = manager.execute(q, fills);
        executeEpilogue(q, success, timer);
        return success;
    }

    public SQLancerResultSet executeStatementAndGet(Query<C> q, String... fills) throws Exception {
        ExecutionTimer timer = executePrologue(q);
        SQLancerResultSet result = manager.executeAndGet(q, fills);
        boolean success = result != null;
        if (success) {
            result.registerEpilogue(() -> {
                try {
                    executeEpilogue(q, success, timer);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        }
        return result;
    }

    public S getSchema() {
        if (schema == null) {
            try {
                updateSchema();
            } catch (Exception e) {
                throw new AssertionError(e.getMessage());
            }
        }
        return schema;
    }

    protected void setSchema(S schema) {
        this.schema = schema;
    }

    public void updateSchema() throws Exception {
        setSchema(readSchema());
        for (AbstractTable<?, ?, ?> table : schema.getDatabaseTables()) {
            table.recomputeCount();
        }
    }

    protected abstract S readSchema() throws Exception;

}
