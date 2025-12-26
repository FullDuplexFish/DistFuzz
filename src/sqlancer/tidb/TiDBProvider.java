package sqlancer.tidb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.AbstractSeedPool;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DecodedStmt;
import sqlancer.common.DecodedStmt.stmtType;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.gen.TiDBAlterTableGenerator;
import sqlancer.tidb.gen.TiDBAnalyzeTableGenerator;
import sqlancer.tidb.gen.TiDBDeleteGenerator;
import sqlancer.tidb.gen.TiDBDropTableGenerator;
import sqlancer.tidb.gen.TiDBDropViewGenerator;
import sqlancer.tidb.gen.TiDBIndexGenerator;
import sqlancer.tidb.gen.TiDBInsertGenerator;
import sqlancer.tidb.gen.TiDBSetGenerator;
import sqlancer.tidb.gen.TiDBTableGenerator;
import sqlancer.tidb.gen.TiDBUpdateGenerator;
import sqlancer.tidb.gen.TiDBViewGenerator;
import sqlancer.tidb.visitor.TiDBVisitor;

@AutoService(DatabaseProvider.class)
public class TiDBProvider extends SQLProviderAdapter<TiDBGlobalState, TiDBOptions> {

    public TiDBProvider() {
        super(TiDBGlobalState.class, TiDBOptions.class);
    }

    public enum Action implements AbstractAction<TiDBGlobalState> {
        CREATE_TABLE(TiDBTableGenerator::createRandomTableStatement), // 0
        CREATE_INDEX(TiDBIndexGenerator::getQuery), // 1
        VIEW_GENERATOR(TiDBViewGenerator::getQuery), // 2
        INSERT(TiDBInsertGenerator::getQuery), // 3
        ALTER_TABLE(TiDBAlterTableGenerator::getQuery), // 4
        TRUNCATE((g) -> new SQLQueryAdapter("TRUNCATE " + g.getSchema().getRandomTable(t -> !t.isView()).getName())), // 5
        UPDATE(TiDBUpdateGenerator::getQuery), // 6
        DELETE(TiDBDeleteGenerator::getQuery), // 7
        SET(TiDBSetGenerator::getQuery), // 8
        ADMIN_CHECKSUM_TABLE(
                (g) -> new SQLQueryAdapter("ADMIN CHECKSUM TABLE " + g.getSchema().getRandomTable().getName())), // 9
        ANALYZE_TABLE(TiDBAnalyzeTableGenerator::getQuery), // 10
        DROP_TABLE(TiDBDropTableGenerator::dropTable), // 11
        DROP_VIEW(TiDBDropViewGenerator::dropView); // 12

        private final SQLQueryProvider<TiDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<TiDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(TiDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static class TiDBGlobalState extends SQLGlobalState<TiDBOptions, TiDBSchema> {
        public boolean hasPolicy = false;
        private TiDBSeedPool seedPool;

        public void setSeedPool(TiDBSeedPool seedPool) {
            this.seedPool = seedPool;
        }
        public TiDBSeedPool getSeedPool() {
            return seedPool;
        }
        @Override
        protected TiDBSchema readSchema() throws SQLException {
            return TiDBSchema.fromConnection(getConnection(), getDatabaseName());
        }
        public void addHistoryToSeedPool() {
            for(String str: getHistory()) {
                /*DecodedStmt decodedStmt = TiDBSQLParser.parse(stmt, getDatabaseName());
                if(decodedStmt.getStmtType() == stmtType.DDL) {
                    getSeedPool().addToDDLSeedPool(decodedStmt);
                }else if(decodedStmt.getStmtType() == stmtType.DML) {
                    getSeedPool().addToDMLSeedPool(decodedStmt);
                }else if(decodedStmt.getStmtType() == stmtType.DQL) {
                    getSeedPool().addToDQLSeedPool(decodedStmt);
                }*/
                if(str.toLowerCase().startsWith("create")) {
                    getSeedPool().addToDDLSeedPool(str);
                }else if(str.toLowerCase().startsWith("select")) {
                    getSeedPool().addToDQLSeedPool(str);
                }else{
                    getSeedPool().addToDMLSeedPool(str);
                }
            }
        }
        public String getRandomIntColumnString(String table_name) {
            try {
                List<TiDBColumn> databaseColumns = TiDBSchema.getTableColumns(getConnection(), table_name);
                
                TiDBColumn col = this.getRandomly().fromList(databaseColumns);
                List<TiDBColumn> list = new ArrayList<TiDBColumn>();
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
        public String getSQLQueriesByGeneration() {
            TiDBTables tables = this.getSchema().getRandomTableNonEmptyTables();
            TiDBExpressionGenerator gen = new TiDBExpressionGenerator(this).setColumns(tables.getColumns());
            TiDBSelect select = new TiDBSelect();

            List<TiDBExpression> fetchColumns = new ArrayList<>();
            fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream().map(c -> new TiDBColumnReference(c))
                    .collect(Collectors.toList()));
            select.setFetchColumns(fetchColumns);

            List<TiDBExpression> tableList = tables.getTables().stream().map(t -> new TiDBTableReference(t))
                    .collect(Collectors.toList());
            List<TiDBExpression> joins = TiDBJoin.getJoins(tableList, this).stream().collect(Collectors.toList());
            select.setJoinList(joins);
            select.setFromList(tableList);
            if (Randomly.getBoolean()) {
                select.setWhereClause(gen.generateExpression());
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                select.setOrderByClauses(gen.generateOrderBys());
            }
            if (Randomly.getBoolean()) {
                select.setLimitClause(gen.generateExpression());
            }
            if (Randomly.getBoolean()) {
                select.setOffsetClause(gen.generateExpression());
            }

            String originalQueryString = TiDBVisitor.asString(select);
            return originalQueryString;
        }
        public List<String> getSQLQueries() {
            List<String> res = new ArrayList<String>();
            if(this.getRandomly().getBoolean() && this.getDbmsSpecificOptions().useSeedPool == true) {//get queries by seed pool
                for(int i = 0; i < ((TiDBOptions)this.getDbmsSpecificOptions()).queriesPerBatch; i ++ ) {
                    res.add(TiDBSQLParser.refineSQL(this.getSeedPool().getDQLSeed().toLowerCase()));
                }
                
            }else{                                //get queries by generation
                for(int i = 0; i < ((TiDBOptions)this.getDbmsSpecificOptions()).queriesPerBatch; i ++ ) {
                    res.add(getSQLQueriesByGeneration().toLowerCase());
                }
            }
            return res;
        }
        public List<String> mutateSQLQueries(List<String> sqls) {
            if(!getDbmsSpecificOptions().enableMutate) {
                return sqls;
            }
            List<String> res = new ArrayList<String>();
            for(String sql: sqls) {
                res.addAll(new TiDBMutator(this, sql).mutateSQL());
            }
            return res;
        }
        /*
         * This function return random columns like 'col1,col2;NUM,TEXT'
         */
        public String getRandomColumnStrings(String table_name) {
            try {
                List<TiDBColumn> databaseColumns = TiDBSchema.getTableColumns(getConnection(), table_name);
                String res = "";
                String tp = "";
                //getLogger().writeCurrent("querying the columns of " + table_name);
                //getLogger().writeCurrent("size is " + databaseColumns.size());
                if(databaseColumns.size() == 1) {
                    TiDBColumn col = this.getRandomly().fromList(databaseColumns);
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

    private static int mapActions(TiDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case ANALYZE_TABLE:
        case CREATE_INDEX:
            return r.getInteger(0, 2);
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case TRUNCATE:
        case DELETE:
        case ADMIN_CHECKSUM_TABLE:
            return r.getInteger(0, 2);
        case SET:
        case UPDATE:
            return r.getInteger(0, 5);
        case VIEW_GENERATOR:
            // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/8
            return r.getInteger(0, 2);
        case ALTER_TABLE:
            return r.getInteger(0, 10); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/10
        case CREATE_TABLE:
        case DROP_TABLE:
        case DROP_VIEW:
            return 0;
        default:
            throw new AssertionError(a);
        }

    }
    public void initDatabase(TiDBGlobalState globalState) throws Exception {
        System.out.println("start init database");
        globalState.setSeedPool(TiDBSeedPool.getInstance(globalState));
        //globalState.getLogger().writeCurrent("starting init seedpool");
        globalState.getSeedPool().initPool("./src/sqlancer/tidb/TiDBSeeds");
        //globalState.getLogger().writeCurrent("finish init seedpool");
        System.out.println("finish load seed");
        
        if(globalState.hasPolicy == false){
            try{
                ExpectedErrors errors = new ExpectedErrors();
                errors.add("already exists");
                globalState.executeStatement(new SQLQueryAdapter("create placement policy p1 primary_region=\"region1\", regions=\"region1,region2, region3\";", errors));
                globalState.executeStatement(new SQLQueryAdapter("create placement policy p2 primary_region=\"region2\", regions=\"region1,region2, region3\";", errors));
                globalState.executeStatement(new SQLQueryAdapter("create placement policy p2 primary_region=\"region3\", regions=\"region1,region2, region3\";", errors));
                globalState.executeStatement(new SQLQueryAdapter("CREATE PLACEMENT POLICY two_replicas FOLLOWERS=2;", errors));

                globalState.executeStatement(new SQLQueryAdapter("ALTER RANGE global PLACEMENT POLICY two_replicas;"));
                globalState.executeStatement(new SQLQueryAdapter("ALTER RANGE global PLACEMENT POLICY two_replicas;"));
                
            }catch(Exception e) {
                e.printStackTrace();
            }
            globalState.hasPolicy = true;
        }
        try{
            
            globalState.executeStatement(new SQLQueryAdapter("SET GLOBAL tidb_multi_statement_mode='ON';"));
            globalState.executeStatement(new SQLQueryAdapter("SET @@sql_mode='';"));
            globalState.executeStatement(new SQLQueryAdapter("SET @@global.tidb_enable_clustered_index='off';"));
            globalState.executeStatement(new SQLQueryAdapter("delete from mysql.opt_rule_blacklist;"));
            globalState.executeStatement(new SQLQueryAdapter("admin reload opt_rule_blacklist;"));
        }catch(Exception e) {
            e.printStackTrace();
        }

    }
    public void updateExpectedErrors(ExpectedErrors errors) {
        TiDBErrors.addExpressionErrors(errors);
        TiDBErrors.addExpressionHavingErrors(errors);
        TiDBErrors.addInsertErrors(errors);
        errors.add("already exists");
    
        errors.addRegex(Pattern.compile("Operation .+ failed"));
        errors.add("The used command is not allowed with this MySQL version");
        errors.add("Unknown table");
        errors.add("expression should not be an empty string");
        errors.add("Partition column values of incorrect type");
        errors.add("Table partition metadata not correct, neither partition expression or list of partition columns");
        errors.add("Duplicate key name");
        errors.add("is of a not allowed type for this type of partitioning");
        errors.add("doesn't exist");
        errors.add("is not granted to root");
        errors.add("cannot issue statements that do not produce result sets");
        errors.add("Global Index is needed for index");
        errors.add("Incorrect index name");
        errors.add("Inconsistency in usage of column lists for partitioning");
        errors.add("index 'PRIMARY' is unique and contains all partitioning columns, but has Global Index set");
        errors.add("database exists");
        errors.add("A CLUSTERED INDEX must include all columns in the table's partitioning function");
        errors.add("Split table region lower value count should be 2");
        errors.add("doesn't exist");
        errors.add("No database selected");
        errors.add("not BASE TABLE");
        errors.add("Information schema is changed during the execution of the statement");
        errors.add("A CLUSTERED INDEX must include all columns in the table's partitioning function");
        errors.add("Invalid default value");
        errors.add("used in key specification without a key length");
        errors.add("Generated column can refer only to generated columns defined prior to it");
        errors.add("All parts of a PRIMARY KEY must be NOT NULL");
        errors.add("Multiple primary key defined");
        errors.add("partition function is not allowed");
    }
    List<String> mutateSeed(TiDBGlobalState state, String sql) {
        TiDBMutator mutator = new TiDBMutator(state, sql);
        List<String> res = mutator.mutateSQL();
        return res;
    }
    @Override
    public void generateDatabase(TiDBGlobalState globalState) throws Exception {
        TiDBSQLParser.setGlobalState(globalState);
        initDatabase(globalState);
        updateExpectedErrors(globalState.getExpectedErrors());
        globalState.setSeedPool(TiDBSeedPool.getInstance(globalState));
        if(globalState.getHistory() == null) {
            globalState.initHistory();
        }
        globalState.clearHistory();
        for (int i = 0; i < Randomly.fromOptions(1, 3); i++) {
            boolean allSuccess = false;
            do {
                SQLQueryAdapter qt = null;
                if(globalState.getRandomly().getBoolean() && !globalState.getSeedPool().getDDLSeedPool().isEmpty() && globalState.getDbmsSpecificOptions().useSeedPool == true) {//优先跑种子池
                    qt = new SQLQueryAdapter(globalState.getSeedPool().getDDLSeed(), globalState.getExpectedErrors(), true);
                }else{
                    qt = new TiDBTableGenerator().getQuery(globalState);
                }
                List<String> mutatedSeed = List.of(qt.getQueryString());
                if(globalState.getDbmsSpecificOptions().enableMutate) {
                    mutatedSeed = mutateSeed(globalState, qt.getQueryString());
                }
                
                for(String sql: mutatedSeed) {
                    qt = new SQLQueryAdapter(sql, globalState.getExpectedErrors(), true);
                    boolean success = globalState.executeStatement(qt);
                    if(success) {
                        allSuccess = true;
                        globalState.insertIntoHistory(qt.getQueryString());
                    }

                }
                
            } while (!allSuccess);
        }
        if(globalState.getRandomly().getBoolean() && globalState.getSeedPool().getDMLSeedPool().size() > 0 && globalState.getDbmsSpecificOptions().useSeedPool == true) {
            

            try{
                for(int i = 0; i < Math.min(globalState.getRandomly().getNotCachedInteger(3, 10), globalState.getSeedPool().getDMLSeedPool().size()); i ++ ) {
                    SQLQueryAdapter qt = new SQLQueryAdapter(globalState.getSeedPool().refineSQL(globalState.getSeedPool().getDMLSeed()), globalState.getExpectedErrors(), true);
                    boolean success = globalState.executeStatement(qt);
                    if(success) {
                        globalState.insertIntoHistory(qt.getQueryString());
                    }
                }
            }catch(Exception e) {
                e.printStackTrace();
            }
        }else{
            StatementExecutor<TiDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                TiDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
            try {
                se.executeStatements();
            } catch (SQLException e) {
                if (e.getMessage().contains(
                        "references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")) {
                    throw new IgnoreMeException(); // TODO: drop view instead
                } else {
                    throw new AssertionError(e);
                }
            }
        }
        

        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == TiDBOracleFactory.CERT)) {
            // Disable strict Group By constraints for ROW oracle
            globalState.executeStatement(new SQLQueryAdapter(
                    "SET @@sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"));

            // Enfore statistic collected for all tables
            ExpectedErrors errors = new ExpectedErrors();
            TiDBErrors.addExpressionErrors(errors);
            for (TiDBTable table : globalState.getSchema().getDatabaseTables()) {
                if (!table.isView()) {
                    globalState.executeStatement(new SQLQueryAdapter("ANALYZE TABLE " + table.getName() + ";", errors));
                }
            }
        }

        // TiFlash replication settings
        if (globalState.getDbmsSpecificOptions().tiflash) {
            ExpectedErrors errors = new ExpectedErrors();
            TiDBErrors.addExpressionErrors(errors);
            for (TiDBTable table : globalState.getSchema().getDatabaseTables()) {
                if (!table.isView()) {
                    globalState.executeStatement(
                            new SQLQueryAdapter("ALTER TABLE " + table.getName() + " SET TIFLASH REPLICA 1;", errors));
                }
            }
            if (Randomly.getBoolean()) {
                globalState.executeStatement(new SQLQueryAdapter("set @@tidb_enforce_mpp=1;"));
            }
        }
        //System.out.println("finish generate database");
    }
    

    @Override
    public SQLConnection createDatabase(TiDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = TiDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = TiDBOptions.DEFAULT_PORT;
        }

        String databaseName = globalState.getDatabaseName();
        String url = String.format("jdbc:mysql://%s:%d/", host, port);
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        globalState.getState().logStatement("USE test");
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        String createDatabaseCommand = "CREATE DATABASE " + databaseName;
        globalState.getState().logStatement(createDatabaseCommand);
        globalState.getState().logStatement("USE " + databaseName);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
            if (globalState.getDbmsSpecificOptions().nonPreparePlanCache) {
                s.execute("set global tidb_enable_non_prepared_plan_cache=ON;");
            }
        }
        try (Statement s = con.createStatement()) {
            s.execute(createDatabaseCommand);
        }
        con.close();
        con = DriverManager.getConnection(url + databaseName, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "tidb";
    }

    @Override
    public String getQueryPlan(String selectStr, TiDBGlobalState globalState) throws Exception {
        String queryPlan = "";
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(selectStr);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        SQLQueryAdapter q = new SQLQueryAdapter("EXPLAIN FORMAT=brief " + selectStr);
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    String targetQueryPlan = rs.getString(1).replace("├─", "").replace("└─", "").replace("│", "").trim()
                            + ";"; // Unify format
                    queryPlan += targetQueryPlan;
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        return queryPlan;
    }

    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[Action.values().length];
    }

    @Override
    protected void executeMutator(int index, TiDBGlobalState globalState) throws Exception {
        SQLQueryAdapter queryMutateTable = Action.values()[index].getQuery(globalState);
        globalState.executeStatement(queryMutateTable);
    }

    @Override
    public boolean addRowsToAllTables(TiDBGlobalState globalState) throws Exception {
        List<TiDBTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (TiDBTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = TiDBInsertGenerator.getQuery(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
