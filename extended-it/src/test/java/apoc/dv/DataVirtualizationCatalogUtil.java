package apoc.dv;

import org.jetbrains.annotations.NotNull;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.getUrlFileName;
import static org.junit.Assert.assertEquals;

public class DataVirtualizationCatalogUtil {
    public static final String AGE_KEY = "age";
    public static final String APOC_DV_QUERY_PARAMS_KEY = "queryParams";
    public static final String CONFIG_KEY = "config";
    public static final Map<String, Object> CONFIG_VALUE = Map.of("header", true);
    public static final String CSV_NAME_VALUE = "csv_vr";
    public static final String CSV_TEST_FILE = "test.csv";
    public static final String DATABASE_NAME = "databaseName";
    public static final String DESC_KEY = "desc";
    public static final String DESC_VALUE = "person's details";
    public static final List<String> EXPECTED_LIST = List.of("$name", "$head_of_state", "$CODE2");
    public static final List<String> EXPECTED_LIST_SORTED = List.of("$name", "$head_of_state", "$CODE2").stream().sorted().toList();
    public static final String FILE_URL = getUrlFileName(CSV_TEST_FILE).toString();
    public static final String HOOK_NODE_NAME_KEY = "hookNodeName";
    public static final String HOOK_NODE_NAME_VALUE = "node to test linking";
    public static final String JDBC_VALUE = "JDBC";
    public static List<String> LABELS = List.of("Person");
    public static final String LABELS_KEY = "labels";
    public static final String LABELS_VALUE = "Person";
    public static final String NAME_KEY = "name";
    public static final String NODE_KEY = "node";
    public static final String PARAMS_KEY = "params";
    public static final List<String> PARAMS_VALUE = List.of("$name", "$age");
    public static final String PERSON_NAME = "Rana";
    public static final String PERSON_AGE = "11";
    public static final String QUERY_KEY = "query";
    public static final String QUERY_VALUE = "map.name = $name and map.age = $age";
    public static final String RELTYPE_KEY = "relType";
    public static final String RELTYPE_VALUE = "LINKED_TO";
    public static final String TYPE_KEY = "type";
    public static final String TYPE_VALUE = "CSV";
    public static final String URL_KEY = "url";
    public static Map<String, Object> MAP_VALUE = Map.of("type", "CSV",
            "url", CSV_TEST_FILE, "query", QUERY_VALUE,
            "desc", DESC_VALUE,
            "labels", LABELS);
    // Virtualize JDBC
    public static final String VIRTUALIZE_JDBC_QUERY = "SELECT * FROM country WHERE Name = ?";
    public static final String JDBC_SELECT_QUERY = "SELECT * FROM country WHERE Name = $name";
    public static final String JDBC_SELECT_QUERY_WITH_PARAM = "SELECT * FROM country WHERE Name = $name AND param_with_question_mark = ? ";

    // Virtualize JDBC With Params Map
    public static final String VIRTUALIZE_JDBC_COUNTRY = "Netherlands";
    public static final String CODE2 = "NL";
    public static final String HEAD_OF_STATE = "Beatrix";
    public static final List<String> VIRTUALIZE_JDBC_APOC_PARAMS = List.of(VIRTUALIZE_JDBC_COUNTRY);
    public static final Map<String, Object> VIRTUALIZE_JDBC_QUERY_PARAMS = Map.of(NAME_KEY, VIRTUALIZE_JDBC_COUNTRY, "CODE2", CODE2, "head_of_state", HEAD_OF_STATE);
    public static final Map<String, Object> VIRTUALIZE_JDBC_QUERY_WRONG_PARAMS = Map.of("foo", VIRTUALIZE_JDBC_COUNTRY, "bar", CODE2, "baz", HEAD_OF_STATE);
    public static final String VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE = "LINKED_TO_NEW";
    public static final String JDBC_NAME = "jdbc_vr";
    public static final String JDBC_DESC = "country details";
    public static final List<Label> JDBC_LABELS = List.of(Label.label("Country"));
    public static final List<String> JDBC_LABELS_AS_STRING = List.of("Country");
    public static final String JDBC_SSL_CONFIG = "?useSSL=false";

    public static final String VIRTUALIZE_JDBC_WITH_PARAMS_QUERY = "SELECT * FROM country WHERE Name = $name AND HeadOfState = $head_of_state AND Code2 = $CODE2";
    public static final String APOC_DV_JDBC_WITH_PARAMS_QUERY = "CALL apoc.dv.query($name, {name: 'Italy', head_of_state: '', CODE2: ''}, $config)";

    // APOC Queries
    public static final String APOC_DV_ADD_QUERY = "CALL apoc.dv.catalog.add($name, $map)";
    public static final String APOC_DV_DROP_QUERY = "CALL apoc.dv.catalog.drop($name, $databaseName)";
    public static final String APOC_DV_INSTALL_QUERY = "CALL apoc.dv.catalog.install($name, $databaseName, $map)";
    public static final Map<String, Object> APOC_DV_INSTALL_PARAMS = Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME, "name", CSV_NAME_VALUE, "map", MAP_VALUE);
    public static final String APOC_DV_QUERY = "CALL apoc.dv.query($name, $queryParams, $config)";
    public static final String APOC_DV_QUERY_WITH_PARAM = "CALL apoc.dv.query($name, ['Italy'], $config)";
    public static final String APOC_DV_QUERY_AND_LINK_QUERY = "MATCH (hook:Hook) WITH hook " +
            "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
            "RETURN path ";
    public static final String APOC_DV_SHOW_QUERY = "CALL apoc.dv.catalog.show()";
    public static Map<String, Object> APOC_DV_QUERY_PARAMS = Map.of("name", PERSON_NAME, "age", PERSON_AGE);
    public static final Map<String, Object> APOC_DV_DROP_PARAMS = Map.of(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", CSV_NAME_VALUE);
    public static final Map<String, Object> APOC_DV_QUERY_AND_LINK_QUERY_PARAMS = Map.of(NAME_KEY, CSV_NAME_VALUE, APOC_DV_QUERY_PARAMS_KEY, APOC_DV_QUERY_PARAMS, RELTYPE_KEY, RELTYPE_VALUE, CONFIG_KEY, CONFIG_VALUE);

    public static final String CREATE_HOOK_QUERY = "create (:Hook {name: $hookNodeName})";
    public static final Map<String, Object> CREATE_HOOK_PARAMS = Map.of(HOOK_NODE_NAME_KEY, HOOK_NODE_NAME_VALUE);

    public static Map<String, Object> getAddQueryConfigMap(String url) {
        return Map.of("type", "CSV",
                "url", url, "query", QUERY_VALUE,
                "desc", DESC_VALUE,
                "labels", LABELS);
    }

    public static @NotNull Map<String, Map<String, String>> getJdbcCredentials(JdbcDatabaseContainer mysql) {
        return Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()));
    }

    public static final void assertCatalogContent(Map<String, Object> row, String url) {
        assertEquals(CSV_NAME_VALUE, row.get(NAME_KEY));
        assertEquals(url, row.get(URL_KEY));
        assertEquals(TYPE_VALUE, row.get(TYPE_KEY));
        assertEquals(List.of(LABELS_VALUE), row.get(LABELS_KEY));
        assertEquals(DESC_VALUE, row.get(DESC_KEY));
        assertEquals(QUERY_VALUE, row.get(QUERY_KEY));
        assertEquals(PARAMS_VALUE, row.get(PARAMS_KEY));
    };

    public static void assertDvCatalogAddOrInstall(Map<String, Object> row, String url) {
        assertEquals(JDBC_NAME, row.get(NAME_KEY));
        assertEquals(url, row.get(URL_KEY));
        assertEquals(JDBC_VALUE, row.get(TYPE_KEY));
        assertEquals(JDBC_LABELS_AS_STRING, row.get(LABELS_KEY));
        assertEquals(JDBC_DESC, row.get(DESC_KEY));
        assertEquals(EXPECTED_LIST, row.get(PARAMS_KEY));
    }

    public static void assertDvQueryAndLinkContent(Map<String, Object> row) {
        Path path = (Path) row.get("path");
        Node node = path.endNode();
        assertEquals(VIRTUALIZE_JDBC_COUNTRY, node.getProperty("Name"));
        assertEquals(JDBC_LABELS, node.getLabels());

        Node hook = path.startNode();
        assertEquals(HOOK_NODE_NAME_VALUE, hook.getProperty("name"));
        assertEquals(List.of(Label.label("Hook")), hook.getLabels());

        Relationship relationship = path.lastRelationship();
        assertEquals(hook, relationship.getStartNode());
        assertEquals(node, relationship.getEndNode());
        assertEquals(VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE, relationship.getType().name());
    }

    public static void assertDvQueryContent(Map<String, Object> row, String url) {
        assertEquals(JDBC_NAME, row.get(NAME_KEY));
        assertEquals(url, row.get(URL_KEY));
        assertEquals(JDBC_VALUE, row.get(TYPE_KEY));
        assertEquals(JDBC_LABELS_AS_STRING, row.get(LABELS_KEY));
        assertEquals(JDBC_DESC, row.get(DESC_KEY));
        assertEquals(List.of("?"), row.get(PARAMS_KEY));
    }

    public static void assertVirtualizeCSVQueryAndLinkContent(Map<String, Object> row) {
        Path path = (Path) row.get("path");
        Node node = path.endNode();
        assertEquals(PERSON_NAME, node.getProperty(NAME_KEY));
        assertEquals(PERSON_AGE, node.getProperty(AGE_KEY));
        assertEquals(List.of(Label.label(LABELS_VALUE)), node.getLabels());

        Node hook = path.startNode();
        assertEquals(HOOK_NODE_NAME_VALUE, hook.getProperty(NAME_KEY));
        assertEquals(List.of(Label.label("Hook")), hook.getLabels());

        Relationship relationship = path.lastRelationship();
        assertEquals(hook, relationship.getStartNode());
        assertEquals(node, relationship.getEndNode());
        assertEquals(RELTYPE_VALUE, relationship.getType().name());
    }

    static void assertDVQueryVirtualizeCSV(Map<String, Object> row) {
        Node node = (Node) row.get(NODE_KEY);
        assertEquals(PERSON_NAME, node.getProperty(NAME_KEY));
        assertEquals(PERSON_AGE, node.getProperty(AGE_KEY));
        assertEquals(List.of(Label.label("Person")), node.getLabels());
    }

    public static String getVirtualizeJDBCUrl(JdbcDatabaseContainer mysql) {
        return mysql.getJdbcUrl() + JDBC_SSL_CONFIG;
    }

    public static Map<String, Object> getVirtualizeJDBCParameterMap(JdbcDatabaseContainer mysql, String query) {
        final String url = getVirtualizeJDBCUrl(mysql);
        Map<String, Object> map = Map.of(TYPE_KEY, JDBC_VALUE,
                URL_KEY, url,
                QUERY_KEY, query,
                DESC_KEY, JDBC_DESC,
                LABELS_KEY, JDBC_LABELS_AS_STRING);
        return map;
    }

}
