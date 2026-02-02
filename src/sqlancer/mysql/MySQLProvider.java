package sqlancer.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.gen.MySQLAlterTable;
import sqlancer.mysql.gen.MySQLDeleteGenerator;
import sqlancer.mysql.gen.MySQLDropIndex;
import sqlancer.mysql.gen.MySQLInsertGenerator;
import sqlancer.mysql.gen.MySQLSetGenerator;
import sqlancer.mysql.gen.MySQLTableGenerator;
import sqlancer.mysql.gen.MySQLTruncateTableGenerator;
import sqlancer.mysql.gen.MySQLUpdateGenerator;
import sqlancer.mysql.gen.admin.MySQLFlush;
import sqlancer.mysql.gen.admin.MySQLReset;
import sqlancer.mysql.gen.datadef.MySQLIndexGenerator;
import sqlancer.mysql.gen.tblmaintenance.MySQLAnalyzeTable;
import sqlancer.mysql.gen.tblmaintenance.MySQLCheckTable;
import sqlancer.mysql.gen.tblmaintenance.MySQLChecksum;
import sqlancer.mysql.gen.tblmaintenance.MySQLOptimize;
import sqlancer.mysql.gen.tblmaintenance.MySQLRepair;
import sqlancer.tidb.TiDBMutator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

@AutoService(DatabaseProvider.class)
public class MySQLProvider extends SQLProviderAdapter<MySQLGlobalState, MySQLOptions> {

    public MySQLProvider() {
        super(MySQLGlobalState.class, MySQLOptions.class);
    }

    enum Action implements AbstractAction<MySQLGlobalState> {
        SHOW_TABLES((g) -> new SQLQueryAdapter("SHOW TABLES")), //
        INSERT(MySQLInsertGenerator::insertRow), //
        SET_VARIABLE(MySQLSetGenerator::set), //
        REPAIR(MySQLRepair::repair), //
        OPTIMIZE(MySQLOptimize::optimize), //
        CHECKSUM(MySQLChecksum::checksum), //
        CHECK_TABLE(MySQLCheckTable::check), //
        ANALYZE_TABLE(MySQLAnalyzeTable::analyze), //
        FLUSH(MySQLFlush::create), RESET(MySQLReset::create), CREATE_INDEX(MySQLIndexGenerator::create), //
        ALTER_TABLE(MySQLAlterTable::create), //
        TRUNCATE_TABLE(MySQLTruncateTableGenerator::generate), //
        SELECT_INFO((g) -> new SQLQueryAdapter(
                "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '" + g.getDatabaseName()
                        + "'")), //
        UPDATE(MySQLUpdateGenerator::create), //
        DELETE(MySQLDeleteGenerator::delete), //
        DROP_INDEX(MySQLDropIndex::generate);

        private final SQLQueryProvider<MySQLGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<MySQLGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(MySQLGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(MySQLGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed = 0;
        switch (a) {
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case SHOW_TABLES:
            nrPerformed = r.getInteger(0, 5);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, 10);
            break;
        case REPAIR:
            nrPerformed = r.getInteger(0, 5);
            break;
        case SET_VARIABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case CREATE_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case FLUSH:
            nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
            break;
        case OPTIMIZE:
            // seems to yield low CPU utilization
            nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 5) : 0;
            break;
        case RESET:
            // affects the global state, so do not execute
            nrPerformed = globalState.getOptions().getNumberConcurrentThreads() == 1 ? r.getInteger(0, 1) : 0;
            break;
        case CHECKSUM:
        case CHECK_TABLE:
        case ANALYZE_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case ALTER_TABLE:
            nrPerformed = r.getInteger(0, 10);
            break;
        case TRUNCATE_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case SELECT_INFO:
            nrPerformed = r.getInteger(0, 3);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 10);
            break;
        case DELETE:
            nrPerformed = r.getInteger(0, 10);
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;
    }
    private void initDatabase(MySQLGlobalState globalState) {
        
        List<String> list = new ArrayList<String>();
        if(globalState.getRandomly().getBoolean()) {
            String ban_op = "SET session optimizer_switch = 'index_merge=off,index_merge_union=off,index_merge_sort_union=off,index_merge_intersection=off,engine_condition_pushdown=off,index_condition_pushdown=off,mrr=off,mrr_cost_based=off,block_nested_loop=off,batched_key_access=off,materialization=off,semijoin=off,loosescan=off,firstmatch=off,duplicateweedout=off,subquery_materialization_cost_based=off,use_index_extensions=off,condition_fanout_filter=off,derived_merge=off,use_invisible_indexes=off,skip_scan=off,hash_join=off,subquery_to_derived=off,prefer_ordering_index=off,derived_condition_pushdown=off,hash_set_operations=off';";
            
            list.add(ban_op);
            ban_op = "set session optimizer_prune_level=0";
            list.add(ban_op);
            ban_op = "set session optimizer_search_depth=0";
            list.add(ban_op);
        }else{
            String ban_op = "SET session optimizer_switch = 'index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=on,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=on,skip_scan=on,hash_join=on,subquery_to_derived=on,prefer_ordering_index=on,derived_condition_pushdown=on,hash_set_operations=on';";
            
            list.add(ban_op);
            ban_op = "set session optimizer_prune_level=1";
            list.add(ban_op);
            ban_op = "set session optimizer_search_depth=10";
            list.add(ban_op);
        }
        list.add("set @@sql_mode=''");
        for(String str : list) {
            try{
                globalState.executeStatement(new SQLQueryAdapter(str, globalState.getExpectedErrors(), false));
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    private void updateExpectedErrors(MySQLGlobalState globalState) {
        globalState.getExpectedErrors().add("cannot be null");
        globalState.getExpectedErrors().add("not supported");
        globalState.getExpectedErrors().add("Incorrect DOUBLE value");
        globalState.getExpectedErrors().add("Incorrect FLOAT value");
        globalState.getExpectedErrors().add("Duplicate key name");
        globalState.getExpectedErrors().add("contains a disallowed function");
        globalState.getExpectedErrors().add("used in key specification without a key length");
        globalState.getExpectedErrors().add("does not support the create option");
        globalState.getExpectedErrors().add("Incorrect decimal value");
        globalState.getExpectedErrors().add("Unknown system variable");
        globalState.getExpectedErrors().add("is not allowed in partition function");
        globalState.getExpectedErrors().add("is of a not allowed type for this type of partitioning");
        globalState.getExpectedErrors().add("Truncated incorrect DOUBLE value");
        globalState.getExpectedErrors().add("Out of range value for column");
        globalState.getExpectedErrors().add("doesn't exist");
        globalState.getExpectedErrors().add("The storage engine for the table doesn't support native partitioning");
        globalState.getExpectedErrors().add("A UNIQUE INDEX must include all columns in the table's partitioning function");
        globalState.getExpectedErrors().add("in where clause is ambiguous");
        globalState.getExpectedErrors().add("Incorrect integer value");
        globalState.getExpectedErrors().add("Incorrect usage of");
        globalState.getExpectedErrors().add("A PRIMARY KEY must include all columns in the table's partitioning function");
        globalState.getExpectedErrors().add(" doesn't have this option");
        globalState.getExpectedErrors().add("has a partitioning function dependency and cannot be dropped or renamed");
        globalState.getExpectedErrors().add("The used storage engine cannot index the expression");
        globalState.getExpectedErrors().add("Data truncated for column");
        globalState.getExpectedErrors().add("Truncated incorrect INTEGER value");
        globalState.getExpectedErrors().add("You have an error in your SQL syntax");
        globalState.getExpectedErrors().add("already exists");
        globalState.getExpectedErrors().add("Duplicate entry");
        globalState.getExpectedErrors().add("is ambiguous");
        globalState.getExpectedErrors().add("Not unique table/alias");
        globalState.getExpectedErrors().add("doesn't have a default value");
        globalState.getExpectedErrors().add("A primary key index cannot be invisible");
        globalState.getExpectedErrors().add("Cannot convert string");
        globalState.getExpectedErrors().add("Data truncated for functional index");
        globalState.getExpectedErrors().add("Cannot create a functional index on an expression that returns");
        globalState.getExpectedErrors().add("has a functional index dependency and cannot be dropped or renamed.");
        globalState.getExpectedErrors().add("BIGINT value is out of range");
        globalState.getExpectedErrors().add("Specified key was too long");
        globalState.getExpectedErrors().add("check that column/key exists");
        globalState.getExpectedErrors().add("Can't group on");

    }
    List<String> mutateSeed(MySQLGlobalState state, String sql) {
        MySQLMutator mutator = new MySQLMutator(state, sql);
        List<String> res = mutator.mutateSQL();
        return res;
    }
    @Override
    public void generateDatabase(MySQLGlobalState globalState) throws Exception {
        initDatabase(globalState);
        updateExpectedErrors(globalState);
        if(globalState.getHistory() == null) {
            globalState.initHistory();
        }
        globalState.clearHistory();
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.getNotCachedInteger(2, 6)) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
            SQLQueryAdapter createTable = MySQLTableGenerator.generate(globalState, tableName);
            // boolean success = globalState.executeStatement(createTable);
            
            // if(success) {
            //     globalState.insertIntoHistory(createTable.getQueryString());
            // }
            List<String> mutatedSeed = List.of(createTable.getQueryString().toLowerCase());
            if(globalState.getDbmsSpecificOptions().enableMutate) {
                mutatedSeed = mutateSeed(globalState, createTable.getQueryString());
            }
            
            for(String sql: mutatedSeed) {
                createTable = new SQLQueryAdapter(sql, globalState.getExpectedErrors(), true);
                boolean success = globalState.executeStatement(createTable);
                if(success) {
                    globalState.insertIntoHistory(createTable.getQueryString());
                }

            }

        }

        StatementExecutor<MySQLGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                MySQLProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();

        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == MySQLOracleFactory.CERT)) {
            // Enfore statistic collected for all tables
            ExpectedErrors errors = new ExpectedErrors();
            MySQLErrors.addExpressionErrors(errors);
            for (MySQLTable table : globalState.getSchema().getDatabaseTables()) {
                StringBuilder sb = new StringBuilder();
                sb.append("ANALYZE TABLE ");
                sb.append(table.getName());
                sb.append(" UPDATE HISTOGRAM ON ");
                String columns = table.getColumns().stream().map(MySQLColumn::getName)
                        .collect(Collectors.joining(", "));
                sb.append(columns + ";");
                globalState.executeStatement(new SQLQueryAdapter(sb.toString(), errors));
            }
        }
    }

    @Override
    public SQLConnection createDatabase(MySQLGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = MySQLOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MySQLOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "mysql";
    }

    @Override
    public boolean addRowsToAllTables(MySQLGlobalState globalState) throws Exception {
        List<MySQLTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (MySQLTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = MySQLInsertGenerator.insertRow(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
