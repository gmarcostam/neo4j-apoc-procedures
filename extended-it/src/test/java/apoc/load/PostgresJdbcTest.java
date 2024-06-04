package apoc.load;

import apoc.periodic.Periodic;
import apoc.text.Strings;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

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
        TestUtil.registerProcedure(db, Jdbc.class, Periodic.class, Strings.class);
        db.executeTransactionally("CALL apoc.load.driver('org.postgresql.Driver')");
        db.executeTransactionally("WITH range(0, 100) as list UNWIND list as l CREATE (n:MyNode{id: l})");
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        postgress.stop();
        db.shutdown();
    }

    @Test
    public void testPeriodicIterateIssue4074() throws IOException, InterruptedException {
        Container.ExecResult execResult = postgress.execInContainer("select client_addr, state from pg_stat_activity");
        System.out.println("getStdout=" + execResult.getStdout() + " getStderr=" + execResult.getStderr());

        String query = """
                CALL apoc.periodic.iterate("MATCH (n:MyNode) return n", "WITH n,
                 'insert into nodes (my_id) values [n.id]' AS sql CALL apoc.load.jdbcUpdate($url1,sql, [], { schema: 'test', credentials: { user: '$user', password: '$password' } }) YIELD row AS row2 return row2,n", 
                 {batchsize: 10,parallel: true,config1: $config, url1: $url}) yield batches,total,timeTaken,committedOperations,failedOperations,failedBatches,retries,errorMessages, operations, failedParams, updateStatistics return batches,total,timeTaken, committedOperations,failedOperations,failedBatches,retries,errorMessages,operations,failedParams, updateStatistics
                """
                    .replace("$url1", postgress.getJdbcUrl())
                    .replace("$user", postgress.getUsername())
                    .replace("$password", postgress.getPassword());
        System.out.println(query);
        testCall(db, query, Util.map("url", postgress.getJdbcUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> {
                    System.out.println(row);

                });

        Container.ExecResult execResult1 = postgress.execInContainer("select client_addr, state from pg_stat_activity");
        System.out.println("getStdout=" + execResult1.getStdout() + " getStderr=" + execResult1.getStderr());
    }

    @Test
    public void testPeriodicIterateIssue4074_1() throws IOException, InterruptedException {
//        Container.ExecResult execResult = postgress.execInContainer("-c 'max_connections=200'");
//        postgress.setCommand("postgres", "-c", "max_connections=1000");
//        System.out.println("getStdout=" + execResult.getStdout() + " getStderr=" + execResult.getStderr());

//        String query = """
//                CALL apoc.periodic.iterate("MATCH (n:MyNode) return n", "WITH n,
//                                 'insert into nodes (my_id) values (' + n.id + ')' AS sql CALL apoc.load.jdbcUpdate('jdbc:postgresql://localhost:PORT/test?loggerLevel=OFF',sql, [], { schema: 'test', credentials: { user: 'test', password: 'test' } }) YIELD row AS row2 return row2,n",\s
//                                 {batchsize: 10,parallel: true}) yield batches,total,timeTaken,committedOperations,failedOperations,failedBatches,retries,errorMessages, operations, failedParams, updateStatistics return batches,total,timeTaken, committedOperations,failedOperations,failedBatches,retries,errorMessages,operations,failedParams, updateStatistics
//                """
//                .replace("PORT", String.valueOf(postgress.getMappedPort(5432)));

        String query = """
                CALL apoc.periodic.iterate("MATCH (n:MyNode) return n", "WITH n, apoc.text.format('insert into nodes (my_id) values (\\\\'%d\\\\')',[n.id]) AS sql CALL apoc.load.jdbcUpdate('jdbc:postgresql://test:test@localhost:PORT/test',sql) YIELD row AS row2 return row2,n", {batchsize: 10,parallel: true})  yield batches,total,timeTaken,committedOperations,failedOperations,failedBatches,retries,errorMessages, operations, failedParams, updateStatistics return batches,total,timeTaken, committedOperations,failedOperations,failedBatches,retries,errorMessages,operations,failedParams, updateStatistics
                """
                .replace("PORT", "50534");
//                        .replace("PORT", String.valueOf(postgress.getMappedPort(5432)));

        System.out.println(query);
        testCall(db, query, Map.of(),
                (row) -> {
                    System.out.println(row);

                });

        Container.ExecResult execResult1 = postgress.execInContainer("select client_addr, state from pg_stat_activity");
        System.out.println("getStdout=" + execResult1.getStdout() + " getStderr=" + execResult1.getStderr());
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
    
}
