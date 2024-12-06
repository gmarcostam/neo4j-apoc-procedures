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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.custom.CypherProcedureTestUtil.startDbWithCustomApocConfigs;
import static apoc.dv.DataVirtualizationCatalogUtil.AGE_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_DROP_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_INSTALL_PARAMS;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_INSTALL_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_JDBC_WITH_PARAMS_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_QUERY_AND_LINK_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_QUERY_AND_LINK_QUERY_PARAMS;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_QUERY_PARAMS_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_QUERY_WITH_PARAM;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_SHOW_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.CONFIG_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.CREATE_HOOK_PARAMS;
import static apoc.dv.DataVirtualizationCatalogUtil.CREATE_HOOK_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.CSV_TEST_FILE;
import static apoc.dv.DataVirtualizationCatalogUtil.FILE_URL;
import static apoc.dv.DataVirtualizationCatalogUtil.HOOK_NODE_NAME_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.HOOK_NODE_NAME_VALUE;
import static apoc.dv.DataVirtualizationCatalogUtil.JDBC_LABELS;
import static apoc.dv.DataVirtualizationCatalogUtil.JDBC_NAME;
import static apoc.dv.DataVirtualizationCatalogUtil.JDBC_SELECT_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.JDBC_SELECT_QUERY_WITH_PARAM;
import static apoc.dv.DataVirtualizationCatalogUtil.LABELS_VALUE;
import static apoc.dv.DataVirtualizationCatalogUtil.NAME_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.NODE_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.PERSON_AGE;
import static apoc.dv.DataVirtualizationCatalogUtil.PERSON_NAME;
import static apoc.dv.DataVirtualizationCatalogUtil.RELTYPE_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.TYPE_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_APOC_PARAMS;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_COUNTRY;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_QUERY_PARAMS;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_WITH_PARAMS_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE;
import static apoc.dv.DataVirtualizationCatalogUtil.assertCatalogContent;
import static apoc.dv.DataVirtualizationCatalogUtil.assertDvCatalogAddOrInstall;
import static apoc.dv.DataVirtualizationCatalogUtil.assertDvQueryContent;
import static apoc.dv.DataVirtualizationCatalogUtil.getJdbcCredentials;
import static apoc.dv.DataVirtualizationCatalogUtil.getVirtualizeJDBCParameterMap;
import static apoc.dv.DataVirtualizationCatalogUtil.getVirtualizeJDBCUrl;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
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

    @Before
    public void setUp() throws Exception {
        databaseManagementService = startDbWithCustomApocConfigs(storeDir);
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        FileUtils.copyFile(new File(new URI(FILE_URL).toURL().getPath()), new File(storeDir.getRoot(), CSV_TEST_FILE));
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
        testCallEventually(sysDb, APOC_DV_INSTALL_QUERY,
                APOC_DV_INSTALL_PARAMS,
                (row) -> assertCatalogContent(row, CSV_TEST_FILE), TIMEOUT);

        testCallEventually(sysDb, APOC_DV_SHOW_QUERY,
                (row) -> assertCatalogContent(row, CSV_TEST_FILE), TIMEOUT);

        testCallEventually(db, APOC_DV_QUERY,
                APOC_DV_QUERY_AND_LINK_QUERY_PARAMS,
                (row) -> {
                    Node node = (Node) row.get(NODE_KEY);
                    assertEquals(PERSON_NAME, node.getProperty(NAME_KEY));
                    assertEquals(PERSON_AGE, node.getProperty(AGE_KEY));
                    assertEquals(List.of(Label.label(LABELS_VALUE)), node.getLabels());
                }, TIMEOUT);

        db.executeTransactionally(CREATE_HOOK_QUERY, CREATE_HOOK_PARAMS);

        testCallEventually(db, APOC_DV_QUERY_AND_LINK_QUERY, APOC_DV_QUERY_AND_LINK_QUERY_PARAMS,
                DataVirtualizationCatalogUtil::assertVirtualizeCSVQueryAndLinkContent, TIMEOUT);

    }

    @Test
    public void testVirtualizeJDBC() {
        final String url = getVirtualizeJDBCUrl(mysql);

        testCallEventually(sysDb, APOC_DV_INSTALL_QUERY,
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME, NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_QUERY)),
                (row) -> assertDvQueryContent(row, url), TIMEOUT);

        testCallCountEventually(db, APOC_DV_QUERY_WITH_PARAM, Map.of(
                    NAME_KEY, JDBC_NAME,
                    CONFIG_KEY, getJdbcCredentials(mysql)),
                0,
                TIMEOUT
        );

        testCallEventually(db, APOC_DV_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS,
                        CONFIG_KEY, getJdbcCredentials(mysql)),
                (row) -> {
                    Node node = (Node) row.get(NODE_KEY);
                    assertEquals(VIRTUALIZE_JDBC_COUNTRY, node.getProperty("Name"));
                    assertEquals(JDBC_LABELS, node.getLabels());
                }, TIMEOUT);

        db.executeTransactionally(CREATE_HOOK_QUERY, CREATE_HOOK_PARAMS);

        testCallEventually(db, APOC_DV_QUERY_AND_LINK_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, getJdbcCredentials(mysql)),
                DataVirtualizationCatalogUtil::assertDvQueryAndLinkContent, TIMEOUT);
    }

    @Test
    public void testVirtualizeJDBCWithParameterMap() {
        final String url = getVirtualizeJDBCUrl(mysql);

        testCallEventually(sysDb, APOC_DV_INSTALL_QUERY,
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,NAME_KEY, JDBC_NAME,
                        "map", getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_WITH_PARAMS_QUERY)),
                (row) -> assertDvCatalogAddOrInstall(row, url), TIMEOUT);

        testCallEmpty(db, APOC_DV_JDBC_WITH_PARAMS_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, CONFIG_KEY, getJdbcCredentials(mysql)));


        testCall(db, APOC_DV_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS,
                        CONFIG_KEY, getJdbcCredentials(mysql)),
                (row) -> {
                    Node node = (Node) row.get(NODE_KEY);
                    assertEquals(VIRTUALIZE_JDBC_COUNTRY, node.getProperty("Name"));
                    assertEquals(JDBC_LABELS, node.getLabels());
                });

        db.executeTransactionally(CREATE_HOOK_QUERY, Map.of(HOOK_NODE_NAME_KEY, HOOK_NODE_NAME_VALUE));

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, getJdbcCredentials(mysql)),
                DataVirtualizationCatalogUtil::assertDvQueryAndLinkContent);
    }

    @Test
    public void testRemove() {
        sysDb.executeTransactionally(APOC_DV_INSTALL_QUERY,
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)));

        testCallCountEventually(sysDb, APOC_DV_DROP_QUERY, Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,NAME_KEY, JDBC_NAME), 0, TIMEOUT);
    }

    @Test
    public void testNameAsKey() {
        Map<String, Object> params = Map.of(
                DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
                NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)
        );

        sysDb.executeTransactionally(APOC_DV_INSTALL_QUERY, params);
        sysDb.executeTransactionally(APOC_DV_INSTALL_QUERY, params);
        testResult(sysDb, APOC_DV_SHOW_QUERY,
                (result) -> assertEquals(1, result.stream().count()));
    }

    @Test
    public void testJDBCQueryWithMixedParamsTypes() {
        try {
            sysDb.executeTransactionally(APOC_DV_INSTALL_QUERY,
                    Map.of(
                            DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,NAME_KEY, JDBC_NAME,
                            "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY_WITH_PARAM)
                    )
            );
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            assertEquals("The query is mixing parameters with `$` and `?` please use just one notation", rootCause.getMessage());
        }
    }

    @Ignore
    @Test
    public void testVirtualizeJDBCWithDifferentParameterMap() {
        String name = "jdbc_vr";
        String desc = "country details";
        List<Label> labels = List.of(Label.label("Country"));
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = $name AND HeadOfState = $head_of_state AND Code2 = $CODE2";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of(TYPE_KEY, "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString);

        final List<String> expectedParams = List.of("$name", "$head_of_state", "$CODE2");
        final List<String> sortedExpectedParams = expectedParams.stream()
                .sorted()
                .collect(Collectors.toList());
        testCallEventually(sysDb, "CALL apoc.dv.catalog.install($name, $databaseName, $map)",
                Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,NAME_KEY, name, "map", map),
                (row) -> {
                    assertEquals(name, row.get(NAME_KEY));
                    assertEquals(url, row.get("url"));
                    assertEquals("JDBC", row.get(TYPE_KEY));
                    assertEquals(labelsAsString, row.get("labels"));
                    assertEquals(desc , row.get("desc"));
                    assertEquals(expectedParams, row.get("params"));
                }, TIMEOUT);

        String country = "Netherlands";
        String code2 = "NL";
        String headOfState = "Beatrix";
        Map<String, Object> queryParams = Map.of("foo", country, "bar", code2, "baz", headOfState);

        try {
            db.executeTransactionally(APOC_DV_QUERY,
                    Map.of(NAME_KEY, name, APOC_DV_QUERY_PARAMS_KEY, queryParams,
                            CONFIG_KEY, getJdbcCredentials(mysql)),
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
