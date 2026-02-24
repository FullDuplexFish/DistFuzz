package sqlancer.cockroachdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Main.QueryManager;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.gen.CockroachDBCommentOnGenerator;
import sqlancer.cockroachdb.gen.CockroachDBCreateStatisticsGenerator;
import sqlancer.cockroachdb.gen.CockroachDBDeleteGenerator;
import sqlancer.cockroachdb.gen.CockroachDBDropTableGenerator;
import sqlancer.cockroachdb.gen.CockroachDBDropViewGenerator;
import sqlancer.cockroachdb.gen.CockroachDBIndexGenerator;
import sqlancer.cockroachdb.gen.CockroachDBInsertGenerator;
import sqlancer.cockroachdb.gen.CockroachDBRandomQuerySynthesizer;
import sqlancer.cockroachdb.gen.CockroachDBSetClusterSettingGenerator;
import sqlancer.cockroachdb.gen.CockroachDBSetSessionGenerator;
import sqlancer.cockroachdb.gen.CockroachDBShowGenerator;
import sqlancer.cockroachdb.gen.CockroachDBTableGenerator;
import sqlancer.cockroachdb.gen.CockroachDBTruncateGenerator;
import sqlancer.cockroachdb.gen.CockroachDBUpdateGenerator;
import sqlancer.cockroachdb.gen.CockroachDBViewGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLMutator;
import sqlancer.cockroachdb.CockroachDBSchema.*;

@AutoService(DatabaseProvider.class)
public class CockroachDBProvider extends SQLProviderAdapter<CockroachDBGlobalState, CockroachDBOptions> {

    public CockroachDBProvider() {
        super(CockroachDBGlobalState.class, CockroachDBOptions.class);
    }

    public enum Action {
        CREATE_TABLE(CockroachDBTableGenerator::generate), CREATE_INDEX(CockroachDBIndexGenerator::create), //
        CREATE_VIEW(CockroachDBViewGenerator::generate), //
        CREATE_STATISTICS(CockroachDBCreateStatisticsGenerator::create), //
        INSERT(CockroachDBInsertGenerator::insert), //
        UPDATE(CockroachDBUpdateGenerator::gen), //
        SET_SESSION(CockroachDBSetSessionGenerator::create), //
        SET_CLUSTER_SETTING(CockroachDBSetClusterSettingGenerator::create), //
        DELETE(CockroachDBDeleteGenerator::delete), //
        TRUNCATE(CockroachDBTruncateGenerator::truncate), //
        DROP_TABLE(CockroachDBDropTableGenerator::drop), //
        DROP_VIEW(CockroachDBDropViewGenerator::drop), //
        COMMENT_ON(CockroachDBCommentOnGenerator::comment), //
        SHOW(CockroachDBShowGenerator::show), //
        TRANSACTION((g) -> {
            String s = Randomly.fromOptions("BEGIN", "ROLLBACK", "COMMIT");
            return new SQLQueryAdapter(s, ExpectedErrors.from("there is no transaction in progress",
                    "there is already a transaction in progress", "current transaction is aborted"));
        }), EXPLAIN((g) -> {
            StringBuilder sb = new StringBuilder("EXPLAIN ");
            ExpectedErrors errors = new ExpectedErrors();
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.fromOptions("VERBOSE", "TYPES", "OPT", "DISTSQL", "VEC"));
                sb.append(") ");
                errors.add("cannot set EXPLAIN mode more than once");
                errors.add("unable to vectorize execution plan");
                errors.add("unsupported type");
                errors.add("vectorize is set to 'off'");
            }
            sb.append(CockroachDBRandomQuerySynthesizer.generate(g, Randomly.smallNumber() + 1));
            CockroachDBErrors.addExpressionErrors(errors);
            return new SQLQueryAdapter(sb.toString(), errors);
        }), //
        SCRUB((g) -> new SQLQueryAdapter(
                "EXPERIMENTAL SCRUB table " + g.getSchema().getRandomTable(t -> !t.isView()).getName(),
                // https://github.com/cockroachdb/cockroach/issues/46401
                ExpectedErrors.from("scrub-fk: column \"t.rowid\" does not exist",
                        "check-constraint: cannot access temporary tables of other sessions" /*
                                                                                              * https:// github. com/
                                                                                              * cockroachdb / cockroach
                                                                                              * /issues/ 47031
                                                                                              */))), //
        SPLIT((g) -> {
            StringBuilder sb = new StringBuilder("ALTER INDEX ");
            CockroachDBTable randomTable = g.getSchema().getRandomTable();
            sb.append(randomTable.getName());
            sb.append("@");
            sb.append(randomTable.getRandomIndex());
            if (Randomly.getBoolean()) {
                sb.append(" SPLIT AT VALUES (true), (false);");
            } else {
                sb.append(" SPLIT AT VALUES (NULL);");
            }
            return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("must be of type"));
        });

        private final SQLQueryProvider<CockroachDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<CockroachDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(CockroachDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static class CockroachDBGlobalState extends SQLGlobalState<CockroachDBOptions, CockroachDBSchema> {

        @Override
        protected CockroachDBSchema readSchema() throws SQLException {
            return CockroachDBSchema.fromConnection(getConnection(), getDatabaseName());
        }
        public String getRandomColumnStrings(String table_name) {
            try {
                List<CockroachDBColumn> databaseColumns = CockroachDBSchema.getTableColumns(getConnection(), table_name);
                String res = "";
                String tp = "";
                //getLogger().writeCurrent("querying the columns of " + table_name);
                //getLogger().writeCurrent("size is " + databaseColumns.size());
                if(databaseColumns.size() == 1) {
                    CockroachDBColumn col = this.getRandomly().fromList(databaseColumns);
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
    public void updateExpectedErrors(CockroachDBGlobalState globalState) {
        globalState.getExpectedErrors().add("could not produce a query plan conforming to the MERGE JOIN hint");
        globalState.getExpectedErrors().add("LOOKUP can only be used with INNER or LEFT joins");
        globalState.getExpectedErrors().add("unsupported comparison operator");
        globalState.getExpectedErrors().add("there is no unique or exclusion constraint matching the ON CONFLICT specification");
        globalState.getExpectedErrors().add("invalid cast");
        globalState.getExpectedErrors().add("unknown signature");
        globalState.getExpectedErrors().add("must appear in the GROUP BY clause or be used in an aggregate function");
        globalState.getExpectedErrors().add("cannot determine type of empty array. Consider casting to the desired type, for example ARRAY[]::int[]");
        globalState.getExpectedErrors().add("has type bytes");
        globalState.getExpectedErrors().addRegexString("expected (.)* to have");
        globalState.getExpectedErrors().add("value type bytes doesn't match type bit of");
        globalState.getExpectedErrors().add("non-integer constant in");
        globalState.getExpectedErrors().add("SPLIT AT data column");
        globalState.getExpectedErrors().add("context-dependent operators are not allowed in STORED COMPUTED COLUMN");
        globalState.getExpectedErrors().add("is not in select list");
        globalState.getExpectedErrors().add("incompatible value type:");
        globalState.getExpectedErrors().add("expressions must appear in select list");
        globalState.getExpectedErrors().add("non-integer constant in GROUP BY");
        globalState.getExpectedErrors().add("unsupported binary operator:");
        globalState.getExpectedErrors().add("violates not-null constraint");
        globalState.getExpectedErrors().add("negative value for");
        globalState.getExpectedErrors().add("duplicate key value violates unique constraint");
        globalState.getExpectedErrors().add("JOIN/USING types bit for left and timetz for right cannot be matched for column");
        globalState.getExpectedErrors().add("incompatible type annotation");
        globalState.getExpectedErrors().add("ambiguous call");
        globalState.getExpectedErrors().add("failed to satisfy CHECK constraint");
        globalState.getExpectedErrors().add("index \"\" already contains column");
        globalState.getExpectedErrors().add("VALUES types bit[] and bytes[] cannot be matched");
        globalState.getExpectedErrors().add("doesn't match type");
        globalState.getExpectedErrors().addRegexString("relation (.)* already exists");
        globalState.getExpectedErrors().addRegexString("missing (.)* primary key column");
        globalState.getExpectedErrors().addRegexString("index (.)* does not exist");
        globalState.getExpectedErrors().addRegexString("index (.)* not found");
        globalState.getExpectedErrors().add("cannot write directly to computed column");
        globalState.getExpectedErrors().add("cannot be matched");
        globalState.getExpectedErrors().add("integer out of range for type");
        globalState.getExpectedErrors().add("invalid regular expression: error parsing regexp");
        globalState.getExpectedErrors().add("cannot create a sharded index on a computed column");
        globalState.getExpectedErrors().add("argument of HAVING must be type bool");
        globalState.getExpectedErrors().addRegexString("could not parse (.)* as type");
        globalState.getExpectedErrors().add("ambiguous binary operator");
        globalState.getExpectedErrors().add("STORED COMPUTED COLUMN expression cannot reference computed columns");
        globalState.getExpectedErrors().add("exceeds supported timestamp bounds");
        globalState.getExpectedErrors().add("UPSERT or INSERT...ON CONFLICT command cannot affect row a second time");
        globalState.getExpectedErrors().add("declared partition columns");
        globalState.getExpectedErrors().addRegexString("relation (.)* does not exist");
        globalState.getExpectedErrors().add("could not produce a query plan conforming to the LOOKUP JOIN hint");
        globalState.getExpectedErrors().add("empty range");
    }

    List<String> mutateSeed(CockroachDBGlobalState state, String sql) {
        CockroachDBMutator mutator = new CockroachDBMutator(state, sql);
        List<String> res = mutator.mutateSQL();
        return res;
    }
    @Override
    public void generateDatabase(CockroachDBGlobalState globalState) throws Exception {
        updateExpectedErrors(globalState);
        if(globalState.getHistory() == null) {
            globalState.initHistory();
        }
        globalState.clearHistory();
        QueryManager<SQLConnection> manager = globalState.getManager();
        MainOptions options = globalState.getOptions();
        List<String> standardSettings = new ArrayList<>();
        standardSettings.add("--Don't send automatic bug reports");
        standardSettings.add("SET CLUSTER SETTING debug.panic_on_failed_assertions = true;");
        standardSettings.add("SET CLUSTER SETTING diagnostics.reporting.enabled    = false;");
        standardSettings.add("SET CLUSTER SETTING diagnostics.reporting.send_crash_reports = false;");


        if (globalState.getDbmsSpecificOptions().testHashIndexes) {
            standardSettings.add("set experimental_enable_hash_sharded_indexes='on';");
        }
        if (globalState.getDbmsSpecificOptions().testTempTables) {
            standardSettings.add("SET experimental_enable_temp_tables = 'on'");
        }
        for (String s : standardSettings) {
            manager.execute(new SQLQueryAdapter(s));
        }
        int nrTable = (int)globalState.getRandomly().getNotCachedInteger(1, 4);
        int tableCnt = 0;
        while(tableCnt < nrTable) {
            try{
                SQLQueryAdapter createTable = CockroachDBTableGenerator.generate(globalState);


                List<String> mutatedSeed = List.of(createTable.getQueryString().toLowerCase());
                if(globalState.getDbmsSpecificOptions().enableMutate) {
                    mutatedSeed = mutateSeed(globalState, createTable.getQueryString());
                }
                
                for(String sql: mutatedSeed) {
                    createTable = new SQLQueryAdapter(sql, globalState.getExpectedErrors(), true);
                    boolean success = globalState.executeStatement(createTable);
                    if(success) {
                        globalState.insertIntoHistory(createTable.getQueryString());
                        tableCnt ++ ;
                    }
    
                }
            } catch (IgnoreMeException e) {
                // continue trying
            }

        }

        int[] nrRemaining = new int[Action.values().length];
        List<Action> actions = new ArrayList<>();
        int total = 0;
        for (int i = 0; i < Action.values().length; i++) {
            Action action = Action.values()[i];
            int nrPerformed = 0;
            switch (action) {
            case INSERT:
                nrPerformed = globalState.getRandomly().getInteger(0, options.getMaxNumberInserts());
                break;
            case UPDATE:
            case SPLIT:
                nrPerformed = globalState.getRandomly().getInteger(0, 3);
                break;
            case EXPLAIN:
                nrPerformed = globalState.getRandomly().getInteger(0, 1);
                break;
            case SHOW:
                nrPerformed = 0;
                break;
            case TRUNCATE:
            case DELETE:
            case CREATE_STATISTICS:
                nrPerformed = globalState.getRandomly().getInteger(0, 2);
                break;
            case CREATE_VIEW:
                nrPerformed = globalState.getRandomly().getInteger(0, 2);
                break;
            case SET_SESSION:
            case SET_CLUSTER_SETTING:
                nrPerformed = globalState.getRandomly().getInteger(0, 3);
                break;
            case CREATE_INDEX:
                nrPerformed = globalState.getRandomly().getInteger(0, 10);
                break;
            case COMMENT_ON:
            case SCRUB:
                nrPerformed = 0; /*
                                  * there are a number of open SCRUB bugs, of which
                                  * https://github.com/cockroachdb/cockroach/issues/47116 crashes the server
                                  */
                break;
            case TRANSACTION:
            case CREATE_TABLE:
            case DROP_TABLE:
            case DROP_VIEW:
                nrPerformed = 0; // r.getInteger(0, 0);
                break;
            default:
                throw new AssertionError(action);
            }
            if (nrPerformed != 0) {
                actions.add(action);
            }
            nrRemaining[action.ordinal()] = nrPerformed;
            total += nrPerformed;
        }

        while (total != 0) {
            Action nextAction = null;
            int selection = globalState.getRandomly().getInteger(0, total);
            int previousRange = 0;
            for (int i = 0; i < nrRemaining.length; i++) {
                if (previousRange <= selection && selection < previousRange + nrRemaining[i]) {
                    nextAction = Action.values()[i];
                    break;
                } else {
                    previousRange += nrRemaining[i];
                }
            }
            assert nextAction != null;
            assert nrRemaining[nextAction.ordinal()] > 0;
            nrRemaining[nextAction.ordinal()]--;
            SQLQueryAdapter query = null;
            try {
                boolean success;
                int nrTries = 0;
                do {
                    query = nextAction.getQuery(globalState);
                    SQLQueryAdapter lowerCaseQuery = new SQLQueryAdapter(query.getQueryString().toLowerCase(), globalState.getExpectedErrors(), true);
                    success = globalState.executeStatement(lowerCaseQuery);
                    if(success) {
                        globalState.getHistory().add(lowerCaseQuery.getQueryString());
                    }
                } while (!success && nrTries++ < 1000);
            } catch (IgnoreMeException e) {

            }
            if (query != null && query.couldAffectSchema() && globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
            total--;
        }

        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == CockroachDBOracleFactory.CERT)) {
            // Enfore statistic collected for all tables
            ExpectedErrors errors = new ExpectedErrors();
            CockroachDBErrors.addExpressionErrors(errors);
            for (CockroachDBTable table : globalState.getSchema().getDatabaseTables()) {
                globalState.executeStatement(new SQLQueryAdapter("ANALYZE " + table.getName() + ";", errors));
            }
        }
    }

    @Override
    public SQLConnection createDatabase(CockroachDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = CockroachDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = CockroachDBOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        String url = String.format("jdbc:postgresql://%s:%d/test", host, port);
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        globalState.getState().logStatement("USE test");
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName + " CASCADE");
        String createDatabaseCommand = "CREATE DATABASE " + databaseName;
        globalState.getState().logStatement(createDatabaseCommand);
        globalState.getState().logStatement("USE " + databaseName);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute(createDatabaseCommand);
        }
        con.close();
        con = DriverManager.getConnection(String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName),
                globalState.getOptions().getUserName(), globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "cockroachdb";
    }

    @Override
    public String getQueryPlan(String selectStr, CockroachDBGlobalState globalState) throws Exception {
        String queryPlan = "";
        String explainQuery = "EXPLAIN (OPT) " + selectStr;
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(explainQuery);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        SQLQueryAdapter q = new SQLQueryAdapter(explainQuery);
        boolean afterProjection = false; // Remove the concrete expression after each Projection operator
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    String targetQueryPlan = rs.getString(1).replace("└──", "").replace("├──", "").replace("│", "")
                            .trim() + ";"; // Unify format
                    if (afterProjection) {
                        afterProjection = false;
                        continue;
                    }
                    if (targetQueryPlan.startsWith("projections")) {
                        afterProjection = true;
                    }
                    // Remove all concrete expressions by keywords
                    if (targetQueryPlan.contains(">") || targetQueryPlan.contains("<") || targetQueryPlan.contains("=")
                            || targetQueryPlan.contains("*") || targetQueryPlan.contains("+")
                            || targetQueryPlan.contains("'")) {
                        continue;
                    }
                    queryPlan += targetQueryPlan;
                }
            }
        } catch (AssertionError e) {
            throw new AssertionError("Explain failed: " + explainQuery);
        }

        return queryPlan;
    }

    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[Action.values().length];
    }

    @Override
    protected void executeMutator(int index, CockroachDBGlobalState globalState) throws Exception {
        SQLQueryAdapter queryMutateTable = Action.values()[index].getQuery(globalState);
        globalState.executeStatement(queryMutateTable);
    }

    @Override
    public boolean addRowsToAllTables(CockroachDBGlobalState globalState) throws Exception {
        List<CockroachDBTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (CockroachDBTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = CockroachDBInsertGenerator.insert(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
