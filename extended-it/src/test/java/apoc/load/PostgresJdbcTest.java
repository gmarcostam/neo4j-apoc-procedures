package apoc.load;

import apoc.load.util.LoadJdbcConfig;
import apoc.periodic.Periodic;
import apoc.text.Strings;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.cypher.internal.javacompat.ResultSubscriber;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static apoc.load.util.JdbcUtil.*;
import static org.junit.Assert.assertNotEquals;

public class PostgresJdbcTest extends AbstractJdbcTest {

    // Even if Postgres support the `TIMESTAMP WITH TIMEZONE` type,
    // the JDBC driver doesn't. Please check https://github.com/pgjdbc/pgjdbc/issues/996 and when the issue is closed fix this

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public static JdbcDatabaseContainer postgress;

    @BeforeClass
    public static void setUp() throws Exception {
        postgress = new PostgreSQLContainer().withInitScript("init_postgres.sql");
        postgress.start();
        TestUtil.registerProcedure(db,Jdbc.class, Periodic.class, Strings.class);
        db.executeTransactionally("CALL apoc.load.driver('org.postgresql.Driver')");
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        postgress.stop();
        db.shutdown();
    }

    @Test
    public void testLoadJdbc() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'PERSON',[], $config)", Util.map("url", postgress.getJdbcUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbSelect() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON',[], $config)", Util.map("url", postgress.getJdbcUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbSelectWithArrays() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM ARRAY_TABLE',[], $config)", Util.map("url", postgress.getJdbcUrl(),
                        "config", Util.map("schema", "test",
                                "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (result) -> {
                    Map<String, Object> row = (Map<String, Object>)result.get("row");
                    assertEquals("John", row.get("NAME"));
                    int[] intVals = (int[])row.get("INT_VALUES");
                    assertArrayEquals(intVals, new int[]{1, 2, 3});
                    double[] doubleVals = (double[])row.get("DOUBLE_VALUES");
                    assertArrayEquals(doubleVals, new double[]{ 1.0, 2.0, 3.0}, 0.01);
                });
    }

    @Test
    public void testLoadJdbcUpdate() throws Exception {
        testCall(db, "CALL apoc.load.jdbcUpdate($url,'UPDATE PERSON SET \"SURNAME\" = ? WHERE \"NAME\" = ?', ['DOE', 'John'], $config)",
                Util.map("url", postgress.getJdbcUrl(),
                        "config", Util.map("schema", "test",
                                "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertEquals( Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testLoadJdbcParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON WHERE \"NAME\" = ?',['John'], $config)", //  YIELD row RETURN row
                Util.map("url", postgress.getJdbcUrl(),
                        "config", Util.map("schema", "test",
                                "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testPeriodicIterateWithJdbc() throws InterruptedException {
        var config = Util.map("url", postgress.getJdbcUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword())));

        String query = "WITH range(0, 100) as list UNWIND list as l CREATE (n:MyNode{id: l})";

        testResult(db, query, Result::resultAsString);

        // Redundant, only to reproduce issue 4141
        query = "CALL apoc.load.driver(\"org.postgresql.Driver\")";

        testResult(db, query, Result::resultAsString);

        // We cannot use container.getJdbcUrl() because it does not provide the url with username and password
        String jdbUrl = getUrl(postgress);

        query = """
                CALL apoc.periodic.iterate("MATCH (n:MyNode) return n", "WITH n, apoc.text.format('insert into nodes (my_id) values (\\\\\\'%d\\\\\\')',[n.id]) AS sql CALL apoc.load.jdbcUpdate('$url',sql) YIELD row AS row2 return row2,n", {batchsize: 10,parallel: true})  yield batches,total,timeTaken,committedOperations,failedOperations,failedBatches,retries,errorMessages, operations, failedParams, updateStatistics return batches,total,timeTaken, committedOperations,failedOperations,failedBatches,retries,errorMessages,operations,failedParams, updateStatistics
                """.replace("$url", jdbUrl);

        testResult(db, query, config, r -> assertPeriodicIterate(r));

        TimeUnit.SECONDS.sleep(1);

        assertPgStatActivity(config, jdbUrl);
    }

    private static void assertPgStatActivity(Map<String, Object> config, String jdbUrl) {
        try {
            LoadJdbcConfig loadJdbcConfig = new LoadJdbcConfig(config);
            Connection conn = (Connection) getConnection(jdbUrl, loadJdbcConfig, Connection.class);
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("select client_addr, state from pg_stat_activity");

            while (rs.next()) {
                assertNotEquals("idle", rs.getString(2));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertPeriodicIterate(Result result) {
        Map<String, Object> res = result.next();
        Map<String, Object> operations = (Map<String, Object>) res.get("operations");

        long failed = (Long) operations.get("failed");
        assertEquals(0L, failed);

        long committed = (Long) operations.get("committed");
        assertEquals(101L, committed);
    }

    private static String getUrl(JdbcDatabaseContainer container) {
        return String.format(
                "jdbc:postgresql://%s:%s@%s:%s/%s?loggerLevel=OFF",
                container.getUsername(),
                container.getPassword(),
                container.getContainerIpAddress(),
                container.getMappedPort(5432),
                container.getDatabaseName()
        );
    }
}
