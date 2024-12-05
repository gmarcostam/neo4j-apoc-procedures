package apoc.dv;

import apoc.create.Create;
import apoc.load.Jdbc;
import apoc.load.LoadCsv;
import apoc.util.TestUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_ADD_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_JDBC_WITH_PARAMS_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_QUERY_AND_LINK_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_QUERY_PARAMS;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_QUERY_PARAMS_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.APOC_DV_QUERY_WITH_PARAM;
import static apoc.dv.DataVirtualizationCatalogUtil.CONFIG_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.CONFIG_VALUE;
import static apoc.dv.DataVirtualizationCatalogUtil.CREATE_HOOK_PARAMS;
import static apoc.dv.DataVirtualizationCatalogUtil.CREATE_HOOK_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.CSV_NAME_VALUE;
import static apoc.dv.DataVirtualizationCatalogUtil.EXPECTED_LIST_SORTED;
import static apoc.dv.DataVirtualizationCatalogUtil.HOOK_NODE_NAME_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.HOOK_NODE_NAME_VALUE;
import static apoc.dv.DataVirtualizationCatalogUtil.JDBC_LABELS;
import static apoc.dv.DataVirtualizationCatalogUtil.JDBC_NAME;
import static apoc.dv.DataVirtualizationCatalogUtil.JDBC_SELECT_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.JDBC_SELECT_QUERY_WITH_PARAM;
import static apoc.dv.DataVirtualizationCatalogUtil.NAME_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.NODE_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.RELTYPE_KEY;
import static apoc.dv.DataVirtualizationCatalogUtil.RELTYPE_VALUE;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_APOC_PARAMS;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_COUNTRY;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_QUERY_PARAMS;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_WITH_PARAMS_QUERY;
import static apoc.dv.DataVirtualizationCatalogUtil.VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE;
import static apoc.dv.DataVirtualizationCatalogUtil.assertCatalogContent;
import static apoc.dv.DataVirtualizationCatalogUtil.assertDvCatalogAddOrInstall;
import static apoc.dv.DataVirtualizationCatalogUtil.assertDvQueryContent;
import static apoc.dv.DataVirtualizationCatalogUtil.getAddQueryConfigMap;
import static apoc.dv.DataVirtualizationCatalogUtil.getJdbcCredentials;
import static apoc.dv.DataVirtualizationCatalogUtil.getVirtualizeJDBCParameterMap;
import static apoc.dv.DataVirtualizationCatalogUtil.getVirtualizeJDBCUrl;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCountEventually;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataVirtualizationCatalogTest {

    public static JdbcDatabaseContainer mysql;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
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
        final String url = getUrlFileName("test.csv").toString();

        testCall(db, APOC_DV_ADD_QUERY,
                Map.of(NAME_KEY, CSV_NAME_VALUE, "map", getAddQueryConfigMap(url)),
                (row) -> assertCatalogContent(row, url));

        testCall(db, "CALL apoc.dv.catalog.list()",
                (row) -> assertCatalogContent(row, url));

        testCall(db, APOC_DV_QUERY,
                Map.of(NAME_KEY, CSV_NAME_VALUE, APOC_DV_QUERY_PARAMS_KEY, APOC_DV_QUERY_PARAMS, CONFIG_KEY, CONFIG_VALUE),
                DataVirtualizationCatalogUtil::assertDVQueryVirtualizeCSV);

        db.executeTransactionally(CREATE_HOOK_QUERY, CREATE_HOOK_PARAMS);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                Map.of(NAME_KEY, CSV_NAME_VALUE, APOC_DV_QUERY_PARAMS_KEY, APOC_DV_QUERY_PARAMS, RELTYPE_KEY, RELTYPE_VALUE, CONFIG_KEY, CONFIG_VALUE),
                DataVirtualizationCatalogUtil::assertVirtualizeCSVQueryAndLinkContent);

    }

    @Test
    public void testVirtualizeJDBC() {
        final String url = getVirtualizeJDBCUrl(mysql);

        testCall(db, APOC_DV_ADD_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_QUERY)),
                (row) -> assertDvQueryContent(row, url));

        testCallEmpty(db, APOC_DV_QUERY_WITH_PARAM, Map.of(NAME_KEY, JDBC_NAME,
                CONFIG_KEY, getJdbcCredentials(mysql)));


        testCall(db, APOC_DV_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS,
                        CONFIG_KEY, getJdbcCredentials(mysql)),
                (row) -> {
                    Node node = (Node) row.get(NODE_KEY);
                    assertEquals(VIRTUALIZE_JDBC_COUNTRY, node.getProperty("Name"));
                    assertEquals(JDBC_LABELS, node.getLabels());
                });

        db.executeTransactionally(CREATE_HOOK_QUERY, CREATE_HOOK_PARAMS);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, getJdbcCredentials(mysql)),
                DataVirtualizationCatalogUtil::assertDvQueryAndLinkContent);
    }

    @Test
    public void testVirtualizeJDBCWithParameterMap() {
        final String url = getVirtualizeJDBCUrl(mysql);

        testCall(db, APOC_DV_ADD_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_WITH_PARAMS_QUERY)),
                (row) -> assertDvCatalogAddOrInstall(row, url));

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
        db.executeTransactionally(APOC_DV_ADD_QUERY,
                Map.of("name", JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)));

        testCallCountEventually(db, "CALL apoc.dv.catalog.remove($name)", Map.of("name", JDBC_NAME), 0, 10L);
    }

    @Test
    public void testNameAsKey() {
        Map<String, Object> params = Map.of(
                NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)
        );

        db.executeTransactionally(APOC_DV_ADD_QUERY, params);
        db.executeTransactionally(APOC_DV_ADD_QUERY, params);
        testResult(db, "CALL apoc.dv.catalog.list()",
                Map.of(),
                (result) -> assertEquals(1, result.stream().count()));
    }

    @Test
    public void testJDBCQueryWithMixedParamsTypes() {
        try {
            db.executeTransactionally(APOC_DV_ADD_QUERY,
                    Map.of("name", JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY_WITH_PARAM)));
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            assertEquals("The query is mixing parameters with `$` and `?` please use just one notation", rootCause.getMessage());
        }
    }

    @Test
    public void testVirtualizeJDBCWithDifferentParameterMap() {
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        testCall(db, APOC_DV_ADD_QUERY,
                Map.of("name", JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_WITH_PARAMS_QUERY)),
                (row) -> assertDvCatalogAddOrInstall(row, url));

        String country = "Netherlands";
        String code2 = "NL";
        String headOfState = "Beatrix";
        Map<String, Object> queryParams = Map.of("foo", country, "bar", code2, "baz", headOfState);

        try {
            db.executeTransactionally(APOC_DV_QUERY,
                    Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, queryParams,
                            CONFIG_KEY, getJdbcCredentials(mysql)),
                    Result::resultAsString);
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            final List<String> actualParams = queryParams.keySet().stream()
                    .map(s -> "$" + s)
                    .sorted()
                    .toList();
            assertEquals(String.format("Expected query parameters are %s, actual are %s", EXPECTED_LIST_SORTED, actualParams), rootCause.getMessage());
        }
    }
}
