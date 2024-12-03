package apoc.dv;

import apoc.create.Create;
import apoc.load.Jdbc;
import apoc.load.LoadCsv;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.custom.CypherProcedureTestUtil.startDbWithCustomApocConfigs;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCountEventually;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testCallEventually;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataVirtualizationCatalogNewProcedureTest {

    private static final String DATABASE_NAME = "databaseName";
    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    public static JdbcDatabaseContainer mysql;

    @Rule
    public TemporaryFolder storeDir = new TemporaryFolder();
    private static final String TEST_FILE = "test.csv";
    private static final String URL = getUrlFileName(TEST_FILE).toString();


    @Before
    public void setUp() throws Exception {
        databaseManagementService = startDbWithCustomApocConfigs(storeDir);
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        FileUtils.copyFile(new File(new URI(URL).toURL().getPath()), new File(storeDir.getRoot(), TEST_FILE));
        TestUtil.registerProcedure(sysDb, DataVirtualizationCatalogNewProcedures.class);
        TestUtil.registerProcedure(db, DataVirtualizationCatalog.class, Jdbc.class, LoadCsv.class, Create.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
    }

    @BeforeClass
    public static void setUpContainer() {
        mysql = new MySQLContainer().withInitScript("init_mysql.sql");
        mysql.start();
    }

    @AfterClass
    public static void tearDownContainer() {
        mysql.stop();
    }

    @Test
    public void testVirtualizeCSV() {
        final String name = "csv_vr";

        final String desc = "person's details";
        final String query = "map.name = $name and map.age = $age";
        List<String> labels = List.of("Person");
        Map<String, Object> map = Map.of("type", "CSV",
                "url", TEST_FILE, "query", query,
                "desc", desc,
                "labels", labels);

        final Consumer<Map<String, Object>> assertCatalogContent = (row) -> {
            assertEquals(name, row.get("name"));
            assertEquals(TEST_FILE, row.get("url"));
            assertEquals("CSV", row.get("type"));
            assertEquals(List.of("Person"), row.get("labels"));
            assertEquals(desc, row.get("desc"));
            assertEquals(query, row.get("query"));
            assertEquals(List.of("$name", "$age"), row.get("params"));
        };

        // TODO - cambiare tutti i `testCall(db` in `testCallEventually(sysDb`,
        //  `testCallEventually` sta in TestUtil

        /* TODO - CAMBIARE i apoc.dv.catalog.add in apoc.dv.catalog.install('neo4j', ....
              ad esempio questa diventa
                  testCallEventually(syssysDb, "CALL apoc.dv.catalog.install('neo4j', $name, $map)",
                                  Map.of("name", name, "map", map),
                                  assertCatalogContent);
        */
        testCallEventually(sysDb, "CALL apoc.dv.catalog.install($name, $databaseName, $map)",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME, "name", name, "map", map),
                assertCatalogContent, TIMEOUT);

        // todo - stessa cosa per le altre procedure,
        //  quindi questo diventa testCallEventually(syssysDb, 'neo4j', "CALL apoc.dv.catalog.show('neo4j')"
        testCallEventually(sysDb, "CALL apoc.dv.catalog.show()",
                assertCatalogContent, TIMEOUT);

        String personName = "Rana";
        String personAge = "11";

        Map<String, Object> queryParams = Map.of("name", personName, "age", personAge);
        /*testCallEventually(sysDb, "CALL apoc.dv.catalog.getQuery($name, $databaseName, $queryParams, $config)",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "queryParams", queryParams, "config", Map.of("header", true)),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(personName, node.getProperty("name"));
                    assertEquals(personAge, node.getProperty("age"));
                    assertEquals(List.of(Label.label("Person")), node.getLabels());
                }, TIMEOUT);*/
        testCallEventually(db, "CALL apoc.dv.query($name, $queryParams, $config)",
                Map.of("name", name, "queryParams", queryParams, "config", Map.of("header", true)),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(personName, node.getProperty("name"));
                    assertEquals(personAge, node.getProperty("age"));
                    assertEquals(List.of(Label.label("Person")), node.getLabels());
                }, TIMEOUT);

        String hookNodeName = "node to test linking";

        // TODO - i db.executeTransactionally dovrebbero rimanere tali e quali, in teoria
        db.executeTransactionally("create (:Hook {name: $hookNodeName})", Map.of("hookNodeName", hookNodeName));

        final String relType = "LINKED_TO";
        testCallEventually(db, "MATCH (hook:Hook) WITH hook " +
                        "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
                        "RETURN path ",
                Map.of("name", name, "queryParams", queryParams, "relType", relType, "config", Map.of("header", true)),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node node = path.endNode();
                    assertEquals(personName, node.getProperty("name"));
                    assertEquals(personAge, node.getProperty("age"));
                    assertEquals(List.of(Label.label("Person")), node.getLabels());

                    Node hook = path.startNode();
                    assertEquals(hookNodeName, hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(hook, relationship.getStartNode());
                    assertEquals(node, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                }, TIMEOUT);

    }

    @Test
    public void testVirtualizeJDBC() {
        String name = "jdbc_vr";
        String desc = "country details";
        List<Label> labels = List.of(Label.label("Country"));
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = ?";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString);

        testCallEventually(sysDb, "CALL apoc.dv.catalog.install($name, $databaseName, $map)",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "map", map),
                (row) -> {
                    assertEquals(name, row.get("name"));
                    assertEquals(url, row.get("url"));
                    assertEquals("JDBC", row.get("type"));
                    assertEquals(labelsAsString, row.get("labels"));
                    assertEquals(desc, row.get("desc"));
                    assertEquals(List.of("?"), row.get("params"));
                }, TIMEOUT);

        testCallCountEventually(sysDb, "CALL apoc.dv.catalog.getQuery($name, $databaseName, ['Italy'], $config)", Map.of(
                    DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
                "name", name,
                "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                0,
                TIMEOUT
        );

        String country = "Netherlands";
        List<String> queryParams = List.of(country);

        testCallEventually(sysDb, "CALL apoc.dv.catalog.getQuery($name, $databaseName, $queryParams, $config)",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "queryParams", queryParams,
                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(country, node.getProperty("Name"));
                    assertEquals(labels, node.getLabels());
                }, TIMEOUT);

        String hookNodeName = "node to test linking";

        db.executeTransactionally("create (:Hook {name: $hookNodeName})", Map.of("hookNodeName", hookNodeName));

        final String relType = "LINKED_TO_NEW";
        testCallEventually(sysDb, "MATCH (hook:Hook) WITH hook " +
                        "CALL apoc.dv.catalog.getQueryAndLink(hook, $relType, $name, $databaseName, $queryParams, $config) yield path " +
                        "RETURN path ",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "queryParams", queryParams, "relType", relType,
                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node node = path.endNode();
                    assertEquals(country, node.getProperty("Name"));
                    assertEquals(labels, node.getLabels());

                    Node hook = path.startNode();
                    assertEquals(hookNodeName, hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(hook, relationship.getStartNode());
                    assertEquals(node, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                }, TIMEOUT);
    }

    @Test
    public void testVirtualizeJDBCWithParameterMap() {
        String name = "jdbc_vr";
        String desc = "country details";
        List<Label> labels = List.of(Label.label("Country"));
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = $name AND HeadOfState = $head_of_state AND Code2 = $CODE2";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString);

        testCallEventually(sysDb, "CALL apoc.dv.catalog.install($name, $databaseName, $map)",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "map", map),
                (row) -> {
                    assertEquals(name, row.get("name"));
                    assertEquals(url, row.get("url"));
                    assertEquals("JDBC", row.get("type"));
                    assertEquals(labelsAsString, row.get("labels"));
                    assertEquals(desc , row.get("desc"));
                    assertEquals(List.of("$name", "$head_of_state", "$CODE2"), row.get("params"));
                }, TIMEOUT);

        testCallEmpty(sysDb, "CALL apoc.dv.catalog.getQuery($name, $databaseName, {name: 'Italy', head_of_state: '', CODE2: ''}, $config)",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))));

        String country = "Netherlands";
        String code2 = "NL";
        String headOfState = "Beatrix";
        Map<String, Object> queryParams = Map.of("name", country, "CODE2", code2, "head_of_state", headOfState);

        testCallEventually(sysDb, "CALL apoc.dv.catalog.getQuery($name, $databaseName, $queryParams, $config)",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "queryParams", queryParams,
                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(country, node.getProperty("Name"));
                    assertEquals(labels, node.getLabels());
                }, TIMEOUT);

        String hookNodeName = "node to test linking";

        db.executeTransactionally("create (:Hook {name: $hookNodeName})", Map.of("hookNodeName", hookNodeName));

        final String relType = "LINKED_TO_NEW";
        testCallEventually(sysDb, "MATCH (hook:Hook) WITH hook " +
                        "CALL apoc.dv.catalog.getQueryAndLink(hook, $relType, $name, $databaseName, $queryParams, $config) yield path " +
                        "RETURN path ",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "queryParams", queryParams, "relType", relType,
                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node node = path.endNode();
                    assertEquals(country, node.getProperty("Name"));
                    assertEquals(labels, node.getLabels());

                    Node hook = path.startNode();
                    assertEquals(hookNodeName, hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(hook, relationship.getStartNode());
                    assertEquals(node, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                }, TIMEOUT);
    }

    @Test
    public void testRemove() {
        String name = "jdbc_vr";
        String desc = "country details";
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = $name";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString);

        sysDb.executeTransactionally("CALL apoc.dv.catalog.install($name, $databaseName, $map)",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "map", map));

        testCallCountEventually(sysDb, "CALL apoc.dv.catalog.drop($name, $databaseName)", Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name), 0, TIMEOUT);
    }

    @Test
    public void testNameAsKey() {
        String name = "jdbc_vr";
        String desc = "country details";
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = $name";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString);

        Map<String, Object> params = Map.of(
                DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
                "name", name, "map", map
        );

        sysDb.executeTransactionally("CALL apoc.dv.catalog.install($name, $databaseName, $map)",
                params);
        sysDb.executeTransactionally("CALL apoc.dv.catalog.install($name, $databaseName, $map)",
                params);
        testResult(sysDb, "CALL apoc.dv.catalog.show()",
                (result) -> assertEquals(1, result.stream().count()));
    }

    @Test
    public void testJDBCQueryWithMixedParamsTypes() {
        try {
            String name = "jdbc_vr";
            String desc = "country details";
            List<String> labelsAsString = List.of("Country");
            final String query = "SELECT * FROM country WHERE Name = $name AND param_with_question_mark = ? ";
            final String url = mysql.getJdbcUrl() + "?useSSL=false";
            Map<String, Object> map = Map.of("type", "JDBC",
                    "url", url, "query", query,
                    "desc", desc,
                    "labels", labelsAsString);

            sysDb.executeTransactionally("CALL apoc.dv.catalog.install($name, $databaseName, $map)",
                    Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "map", map));
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            assertEquals("The query is mixing parameters with `$` and `?` please use just one notation", rootCause.getMessage());
        }
    }

    @Test
    public void testVirtualizeJDBCWithDifferentParameterMap() {
        String name = "jdbc_vr";
        String desc = "country details";
        List<Label> labels = List.of(Label.label("Country"));
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = $name AND HeadOfState = $head_of_state AND Code2 = $CODE2";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString);

        final List<String> expectedParams = List.of("$name", "$head_of_state", "$CODE2");
        final List<String> sortedExpectedParams = expectedParams.stream()
                .sorted()
                .collect(Collectors.toList());
        testCallEventually(sysDb, "CALL apoc.dv.catalog.install($name, $databaseName, $map)",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "map", map),
                (row) -> {
                    assertEquals(name, row.get("name"));
                    assertEquals(url, row.get("url"));
                    assertEquals("JDBC", row.get("type"));
                    assertEquals(labelsAsString, row.get("labels"));
                    assertEquals(desc , row.get("desc"));
                    assertEquals(expectedParams, row.get("params"));
                }, TIMEOUT);

        String country = "Netherlands";
        String code2 = "NL";
        String headOfState = "Beatrix";
        Map<String, Object> queryParams = Map.of("foo", country, "bar", code2, "baz", headOfState);

        try {
            sysDb.executeTransactionally("CALL apoc.dv.catalog.getQuery($name, $databaseName, $queryParams, $config)",
                    Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", name, "queryParams", queryParams,
                            "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                    Result::resultAsString);
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            final List<String> actualParams = queryParams.keySet().stream()
                    .map(s -> "$" + s)
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(String.format("Expected query parameters are %s, actual are %s", sortedExpectedParams, actualParams), rootCause.getMessage());
        }
    }
}
