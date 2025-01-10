package apoc.load;

import apoc.periodic.Periodic;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class DuckDBJdbcTest extends AbstractJdbcTest {

    public String JDBC_DUCKDB = null;
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    private Connection conn;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        JDBC_DUCKDB = "jdbc:duckdb:" + temporaryFolder.newFolder() + "/testDB";// UUID.randomUUID();
        apocConfig().setProperty("apoc.jdbc.duckdb.url", JDBC_DUCKDB);
        apocConfig().setProperty("apoc.jdbc.test.sql","SELECT * FROM PERSON");
        apocConfig().setProperty("apoc.jdbc.testparams.sql","SELECT * FROM PERSON WHERE NAME = ?");
        TestUtil.registerProcedure(db, Jdbc.class, Periodic.class, Analytics.class);
        
        conn = DriverManager.getConnection(JDBC_DUCKDB);
        createPersonTableAndData();

        String movies = Util.readResourceFile(MOVIES_CYPHER_FILE);
        try (Transaction tx = db.beginTx()) {
            tx.execute(movies);
            tx.commit();
        }
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    public void testLoadJdbcAnalytics() {
        String cypher = "MATCH (n:Movie) RETURN n.title AS title, n.released AS released, n.language AS language, n.tagline AS tagline";

        String sql = """
            SELECT
            title,
            released,
            language,
            tagline,
            RANK() OVER (PARTITION BY language ORDER BY released DESC) AS rank
            FROM temp_table
            ORDER BY rank, title, tagline;
            """;
        testResult(db, "CALL apoc.load.jdbc.analytics($queryCypher, $url, $sql)",
                map(
                        "queryCypher", cypher,
                        "sql", sql,
                        "url", JDBC_DUCKDB
                ),
                r -> {
                    Map<String, Object> row = r.next();
                    var result = (Map) row.get("row");
                    var rank = (long) result.get("rank");
                    assertEquals(1, rank);

                    row = r.next();
                    result = (Map) row.get("row");
                    rank = (long) result.get("rank");
                    assertEquals(1, rank);

                    row = r.next();
                    result = (Map) row.get("row");
                    rank = (long) result.get("rank");
                    assertEquals(1, rank);

                    row = r.next();
                    result = (Map) row.get("row");
                    rank = (long) result.get("rank");
                    assertEquals(2, rank);

                    row = r.next();
                    result = (Map) row.get("row");
                    rank = (long) result.get("rank");
                    assertEquals(3, rank);

                    row = r.next();
                    result = (Map) row.get("row");
                    rank = (long) result.get("rank");
                    assertEquals(4, rank);

                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testLoadJdbcAnalyticsDuckDBWindow() {
        final String ROW = "row";
        final String IT = "it";
        final String EN = "en";
        final String RELEASED = "released";

        String cypher = "MATCH (n:Movie) RETURN n.title AS title, n.released AS released, n.language AS language, n.tagline AS tagline, n.qty AS qty";

        String sql = """
                WITH ranked_data AS (
                    SELECT
                    title,
                    released,
                    language,
                    tagline,
                    qty,
                    ROW_NUMBER() OVER (PARTITION BY language ORDER BY released DESC) AS rank
                    FROM temp_table
                    ORDER BY rank, title, tagline
                )
                
                SELECT *
                FROM ranked_data
                PIVOT (
                    sum(qty)
                    FOR 
                        language IN ('en', 'it')
                    GROUP BY released
                )
                """;

        testResult(db, "CALL apoc.load.jdbc.analytics($queryCypher, $url, $sql)",
                map(
                        "queryCypher", cypher,
                        "sql", sql,
                        "url", JDBC_DUCKDB
                ),
                r -> {
                    Map<String, Object> row = r.next();
                    var result = (Map) row.get(ROW);
                    var released = (int) result.get(RELEASED);
                    var it = (String) result.get(IT);
                    var en = (String) result.get(EN);
                    assertEquals(1986, released);
                    assertEquals("12", it);
                    assertNull(en);

                    row = r.next();
                    result = (Map) row.get(ROW);
                    released = (int) result.get(RELEASED);
                    it = (String) result.get(IT);
                    en = (String) result.get(EN);
                    assertEquals(1992, released);
                    assertEquals("7", it);
                    assertNull(en);

                    row = r.next();
                    result = (Map) row.get(ROW);
                    released = (int) result.get(RELEASED);
                    it = (String) result.get(IT);
                    en = (String) result.get(EN);
                    assertEquals(1997, released);
                    assertEquals("3", en);
                    assertNull(it);

                    row = r.next();
                    result = (Map) row.get(ROW);
                    released = (int) result.get(RELEASED);
                    it = (String) result.get(IT);
                    en = (String) result.get(EN);
                    assertEquals(1999, released);
                    assertEquals("5", en);
                    assertNull(it);

                    row = r.next();
                    result = (Map) row.get(ROW);
                    released = (int) result.get(RELEASED);
                    it = (String) result.get(IT);
                    en = (String) result.get(EN);
                    assertEquals(2003, released);
                    assertEquals("17", en);
                    assertNull(it);
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testLoadJdbc() {
        testCall(db, "CALL apoc.load.jdbc($url,'PERSON')",
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }

    @Test
    public void testLoadJdbcWithFetchSize() {
        testCall(db, "CALL apoc.load.jdbc($url,'PERSON', null, {fetchSize: 100})",
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }

    @Test
    public void testLoadJdbcSelect() {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON')",
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }
    
    @Test
    public void testLoadJdbcSelectColumnNames() {
        Map<String, Object> expected = map("NAME", "John",
                "DATE", AbstractJdbcTest.hireDate.toLocalDate());
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT NAME, HIRE_DATE AS DATE FROM PERSON')",
                map("url", JDBC_DUCKDB),
                (row) -> assertEquals(expected, row.get("row")));
    }

    @Test
    public void testLoadJdbcParams() {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON WHERE NAME = ?',['John'])", //  YIELD row RETURN row
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }

    @Test
    public void testLoadJdbcParamsWithConfigLocalDateTime() {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON WHERE NAME = ?',['John'])",
                map("url", JDBC_DUCKDB),
                this::assertResult);

        ZoneId asiaTokio = ZoneId.of("Asia/Tokyo");

        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON WHERE NAME = ?',['John'], $config)",
                map("url", JDBC_DUCKDB,
                        "config", map("timezone", asiaTokio.toString())),
                (row) -> {
                    Map<String, Object> expected = MapUtil.map("NAME", "John", "SURNAME", null,
                            "HIRE_DATE", AbstractJdbcTest.hireDate.toLocalDate(),
                            "EFFECTIVE_FROM_DATE", AbstractJdbcTest.effectiveFromDate.toInstant().atZone(asiaTokio).toOffsetDateTime().toZonedDateTime(), // todo investigate why by only changing the procedure mode returned class type changes
                            "TEST_TIME", AbstractJdbcTest.time.toLocalTime(),
                            "NULL_DATE", null);
                    Map<String, Object> rowColumn = (Map<String, Object>) row.get("row");

                    expected.keySet().forEach( k -> {
                        assertEquals(expected.get(k), rowColumn.get(k));
                    });
                    assertEquals(expected, rowColumn);
                }

        );
    }

    @Test
    public void testLoadJdbcParamsWithWrongTimezoneValue() {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Failed to invoke procedure `apoc.load.jdbc`: Caused by: java.lang.IllegalArgumentException: The timezone field contains an error: Unknown time-zone ID: Italy/Pescara");
        TestUtil.singleResultFirstColumn(db,"CALL apoc.load.jdbc('jdbc:duckdb:testDB','SELECT * FROM PERSON WHERE NAME = ?',['John'], {timezone: $timezone})",
                map("timezone", "Italy/Pescara"));
    }

    @Test
    public void testLoadJdbcKey() {
        testCall(db, "CALL apoc.load.jdbc('duckdb','PERSON')",
                this::assertResult);
    }

    @Test
    public void testLoadJdbcSqlAlias() {
        testCall(db, "CALL apoc.load.jdbc('duckdb','test')",
                this::assertResult);
    }

    @Test
    public void testLoadJdbcSqlAliasParams() {
        testCall(db, "CALL apoc.load.jdbc($url,'testparams',['John'])", //  YIELD row RETURN row
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }

    @Test
    public void testLoadJdbcError() {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Invalid input");
        db.executeTransactionally("CALL apoc.load.jdbc(''jdbc:duckdb:testDB'','PERSON2')");
    }

    @Test
    public void testLoadJdbcProcessingError() {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Invalid input");
        db.executeTransactionally("CALL apoc.load.jdbc(''jdbc:duckdb:testDB'','PERSON') YIELD row where row.name / 2 = 5 RETURN row");
    }

    @Test
    public void testLoadJdbcUpdate() {
        testCall(db, "CALL apoc.load.jdbcUpdate($url,'UPDATE PERSON SET SURNAME = ? WHERE NAME = ?', ['DOE', 'John'])",
                map("url", JDBC_DUCKDB),
                (row) -> assertEquals(Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testLoadJdbcUpdateParams() {
        testCall(db, "CALL apoc.load.jdbcUpdate($url,'UPDATE PERSON SET SURNAME = ? WHERE NAME = ?',['John','John'])",
                map("url", JDBC_DUCKDB),
                (row) -> assertEquals(Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testWithPeriodic() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from person");
            stmt.execute("select count(*) as size from person");
            stmt.getResultSet().next();
            int size = stmt.getResultSet().getInt("size");
            assertEquals(0 , size);
        } catch (Exception e) { }

        db.executeTransactionally("UNWIND range(1, 100) AS id CREATE (p:Person{id: id, name: 'Name ' + id, surname: 'Surname ' + id})");
        String query = "CALL apoc.periodic.iterate(\n" +
                "'MATCH (p:Person) RETURN p.name AS name, p.surname AS surname limit 1',\n" +
                "\"CALL apoc.load.jdbcUpdate($url, 'INSERT INTO PERSON(NAME, SURNAME) VALUES(?, ?)', [name, surname]) YIELD row RETURN 'DONE'\",\n" +
                "{batchSize: 20, iterateList: false, params: {url: $url}, parallel: true}\n" +
                ")\n" +
                "YIELD committedOperations, failedOperations, failedBatches, errorMessages\n" +
                "RETURN *";
        testCall(db,
                query,
                map("url", "jdbc:duckdb:testDB"),
                (row) -> {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("select count(*) as size from person");
                        stmt.getResultSet().next();
                        int size = stmt.getResultSet().getInt("size");
                        assertEquals(1, size);
                    } catch (Exception e) { }
                });
    }

    @Test
    public void testIterateJDBC() {
        final String jdbc = "CALL apoc.load.jdbc($url, 'PERSON',[]) YIELD row RETURN row";
        final String create = "CREATE (p:Person) SET p += row";
        testResult(db, "CALL apoc.periodic.iterate($jdbcQuery, $createQuery, {params: $params})",
                Util.map("params", Util.map("url", JDBC_DUCKDB), "jdbcQuery", jdbc, "createQuery", create), result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(1L, row.get("batches"));
                    assertEquals(1L, row.get("total"));
                });

        testCall(db,
                "MATCH (p:Person) return count(p) as count",
                row -> assertEquals(1L, row.get("count"))
        );
    }

    private void createPersonTableAndData() throws SQLException {
        try { conn.createStatement().execute("DROP TABLE PERSON"); } catch (SQLException se) {/*ignore*/}
        conn.createStatement().execute("CREATE TABLE PERSON (NAME varchar(50), SURNAME varchar(50), HIRE_DATE DATE, EFFECTIVE_FROM_DATE TIMESTAMP, TEST_TIME TIME, NULL_DATE DATE)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO PERSON values(?,null,?,?,?,?)");
        ps.setString(1, "John");
        ps.setDate(2, AbstractJdbcTest.hireDate);
        ps.setTimestamp(3, AbstractJdbcTest.effectiveFromDate);
        
        
        // TODO workaround, DuckDB is shifted 1 hour later
        //      VEDERE SE c'è un modo più carino, invece di passare un valore manuale 1 ora indietro a AbstractJdbcTest.time
        //      altrimenti va bene così
        
        // since currenty neither         ps.setTime(4, AbstractJdbcTest.time, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        // or  ps.setObject(1, localTime); is possible
        ps.setTime(4, java.sql.Time.valueOf("16:37:00"));
//        ps.setObject(4, AbstractJdbcTest.time.toLocalTime());//, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        ps.setNull(5, Types.DATE);
        int rows = ps.executeUpdate();
        assertEquals(1, rows);
        ResultSet rs = conn.createStatement().executeQuery("SELECT NAME, HIRE_DATE, EFFECTIVE_FROM_DATE, TEST_TIME FROM PERSON");
        assertEquals(true, rs.next());
        assertEquals("John", rs.getString("NAME"));
        Assert.assertEquals(AbstractJdbcTest.hireDate.toLocalDate(), rs.getDate("HIRE_DATE").toLocalDate());
        Assert.assertEquals(AbstractJdbcTest.effectiveFromDate, rs.getTimestamp("EFFECTIVE_FROM_DATE"));

        // workaround, here the hour is 15:37
        Assert.assertEquals(AbstractJdbcTest.time, rs.getTime("TEST_TIME"));
        assertEquals(false, rs.next());
        rs.close();
    }

}
