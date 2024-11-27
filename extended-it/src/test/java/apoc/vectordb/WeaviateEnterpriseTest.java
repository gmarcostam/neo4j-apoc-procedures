package apoc.vectordb;

import apoc.util.MapUtil;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.driver.Session;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.weaviate.WeaviateContainer;

import java.util.List;
import java.util.Map;

import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testCallEmpty;
import static apoc.util.TestContainerUtil.testResult;
import static apoc.util.Util.map;
import static apoc.vectordb.VectorDbTestUtil.EntityType.FALSE;
import static apoc.vectordb.VectorDbTestUtil.assertBerlinResult;
import static apoc.vectordb.VectorDbTestUtil.assertLondonResult;
import static apoc.vectordb.VectorDbTestUtil.getAuthHeader;
import static apoc.vectordb.VectorEmbeddingConfig.ALL_RESULTS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.FIELDS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;


public class WeaviateEnterpriseTest {
    private static final List<String> FIELDS = List.of("city", "foo");
    private static final String ADMIN_KEY = "jane-secret-key";
    private static final String READONLY_KEY = "ian-secret-key";
    private static final String COLLECTION_NAME = "TestCollection";

    private static final WeaviateContainer WEAVIATE_CONTAINER = new WeaviateContainer("semitechnologies/weaviate:1.24.5")
            .withEnv("AUTHENTICATION_APIKEY_ENABLED", "true")
            .withEnv("AUTHENTICATION_APIKEY_ALLOWED_KEYS", ADMIN_KEY + "," + READONLY_KEY)
            .withEnv("AUTHENTICATION_APIKEY_USERS", "jane@doe.com,ian-smith")

            .withEnv("AUTHORIZATION_ADMINLIST_ENABLED", "true")
            .withEnv("AUTHORIZATION_ADMINLIST_USERS", "jane@doe.com,john@doe.com")
            .withEnv("AUTHORIZATION_ADMINLIST_READONLY_USERS", "ian-smith,roberta@doe.com");

    private static final Map<String, String> ADMIN_AUTHORIZATION = getAuthHeader(ADMIN_KEY);
    private static final Map<String, String> READONLY_AUTHORIZATION = getAuthHeader(READONLY_KEY);
    private static final Map<String, Object> ADMIN_HEADER_CONF = map(HEADERS_KEY, ADMIN_AUTHORIZATION);

    private static final String ID_1 = "8ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308";
    private static final String ID_2 = "9ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308";

    private static String HOST;
    private static final int WEAVIATE_PORT = 8080;

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void setUp() throws Exception {
        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED), true)
                .waitingFor(Wait.forLogMessage(".*Started..*\\n", 1))
                .withNeo4jConfig("dbms.transaction.timeout", "60s");
        neo4jContainer.start();
        session = neo4jContainer.getSession();


        WEAVIATE_CONTAINER.waitingFor(Wait.forLogMessage(".*Serving weaviate at http://.*\\n", 1));

        WEAVIATE_CONTAINER.start();
        HOST = "http://host.docker.internal:" + WEAVIATE_CONTAINER.getMappedPort(WEAVIATE_PORT);

        Map<String, Object> params = map("host", HOST, "conf", ADMIN_HEADER_CONF);
        testCall(session, "CALL apoc.vectordb.weaviate.createCollection($host, 'TestCollection', 'cosine', 4, $conf)",
                params,
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("TestCollection", value.get("class"));
                });

        testResult(session, """
                        CALL apoc.vectordb.weaviate.upsert($host, 'TestCollection',
                        [
                            {id: $id1, vector: [0.05, 0.61, 0.76, 0.74], metadata: {city: "Berlin", foo: "one"}},
                            {id: $id2, vector: [0.19, 0.81, 0.75, 0.11], metadata: {city: "London", foo: "two"}},
                            {id: '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308', vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}},
                            {id: '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309', vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}}
                        ],
                        $conf)
                        """,
                map("host", HOST, "id1", ID_1, "id2", ID_2, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map<String, Object> row = r.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");

                    assertEquals(COLLECTION_NAME, value.get("class"));

                    while (r.hasNext()) {
                        row = r.next();
                        value = (Map<String, Object>) row.get("value");
                        assertEquals(COLLECTION_NAME, value.get("class"));
                    }
                    assertFalse(r.hasNext());
                });

        // -- delete vector
        testCall(session, "CALL apoc.vectordb.weaviate.delete($host, 'TestCollection', " +
                        "['7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308', '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309']" +
                        ", $conf) ",
                map("host", HOST, "conf", ADMIN_HEADER_CONF),
                r -> {
                    List value = (List) r.get("value");
                    assertEquals(List.of("7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308", "7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309"), value);
                });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCallEmpty(session, "CALL apoc.vectordb.weaviate.deleteCollection($host, $collectionName, $conf)",
                MapUtil.map("host", HOST, "collectionName", COLLECTION_NAME, "conf", ADMIN_HEADER_CONF)
        );
        session.close();
        neo4jContainer.close();
        WEAVIATE_CONTAINER.stop();
    }

    @Test
    public void queryVectors() {
        testResult(session, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                        " YIELD score, vector, id, metadata RETURN * ORDER BY id",
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, ID_1, FALSE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, ID_2, FALSE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });
    }
}
