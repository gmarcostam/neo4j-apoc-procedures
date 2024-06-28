package apoc.vectordb;

import apoc.util.MapUtil;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VectorDbTestUtil {
    
    enum EntityType { NODE, REL, FALSE }

    public static int SIZE_PERFORMANCE = 20000;
    
    public static void dropAndDeleteAll(GraphDatabaseService db) {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    public static void assertBerlinResult(Map row, EntityType entityType) {
        assertBerlinResult(row, "1", entityType);
    }
    
    public static void assertBerlinResult(Map row, String id, EntityType entityType) {
        assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
        assertEquals(id, row.get("id").toString());
        if (!entityType.equals(EntityType.FALSE)) {
            String entity = entityType.equals(EntityType.NODE) ? "node" : "rel";
            Map<String, Object> props = ((Entity) row.get(entity)).getAllProperties();
            assertBerlinProperties(props);
        }
    }

    public static void assertLondonResult(Map row, EntityType entityType) {
        assertLondonResult(row, "2", entityType);
    }

    public static void assertLondonResult(Map row, String id, EntityType entityType) {
        assertEquals(Map.of("city", "London", "foo", "two"), row.get("metadata"));
        assertEquals(id, row.get("id").toString());
        if (!entityType.equals(EntityType.FALSE)) {
            String entity = entityType.equals(EntityType.NODE) ? "node" : "rel";
            Map<String, Object> props = ((Entity) row.get(entity)).getAllProperties();
            assertLondonProperties(props);
        }
    }

    public static void assertNodesCreated(GraphDatabaseService db) {
        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                VectorDbTestUtil::vectorEntityAssertions);
    }

    public static void assertRelsCreated(GraphDatabaseService db) {
        testResult(db, "MATCH (:Start)-[r:TEST]->(:End) RETURN properties(r) AS props ORDER BY r.myId",
                VectorDbTestUtil::vectorEntityAssertions);
    }

    public static void vectorEntityAssertions(Result r) {
        ResourceIterator<Map> propsIterator = r.columnAs("props");
        assertBerlinProperties(propsIterator.next());
        assertLondonProperties(propsIterator.next());

        assertFalse(propsIterator.hasNext());
    }

    private static void assertLondonProperties(Map props) {
        assertEquals("London", props.get("city"));
        assertEquals("two", props.get("myId"));
        assertTrue(props.get("vect") instanceof float[]);
    }

    private static void assertBerlinProperties(Map props) {
        assertEquals("Berlin", props.get("city"));
        assertEquals("one", props.get("myId"));
        assertTrue(props.get("vect") instanceof float[]);
    }

    public static Map<String, String> getAuthHeader(String key) {
        return map("Authorization", "Bearer " + key);
    }
    
    public static void assertReadOnlyProcWithMappingResults(Result r, String node) {
        Map<String, Object> row = r.next();
        Map<String, Object> props = ((Entity) row.get(node)).getAllProperties();
        assertEquals(MapUtil.map("readID", "one"), props);
        assertNotNull(row.get("vector"));
        assertNotNull(row.get("id"));

        row = r.next();
        props = ((Entity) row.get(node)).getAllProperties();
        assertEquals(MapUtil.map("readID", "two"), props);
        assertNotNull(row.get("vector"));
        assertNotNull(row.get("id"));

        assertFalse(r.hasNext());
    }

    public static void stopWatchLog(StopWatch watch, String operation) {
        watch.stop();
        System.out.println("Operation: " + operation  + " | Time spent: " + watch.getTime() + "ms");
        watch.reset();
    }

    public static List<Map<String, Object>> generateFakeData(String type) {
        List<Map<String, Object>> data = new ArrayList<>();

        IntStream.range(0, getSizePerformanceVectors(type)).forEach(i -> data.add(
                Map.of(
                        "id", VectorDbHandler.Type.WEAVIATE.name().equals(type) ? UUID.randomUUID().toString() : i,
                        "vector", List.of(
                                Math.random(), Math.random(), Math.random(), Math.random()
                        ),
                        "metadata", Map.of("city", "Berlin", "foo", "one")
                )
        ));

        return data;
    }

    public static int getSizePerformanceVectors(String type) {
        return switch (VectorDbHandler.Type.valueOf(type)) {
            // we can't increase this value, since there is an upsert limit
            // see https://github.com/chroma-core/chroma/issues/1049
            case CHROMA -> 41666;
            case QDRANT -> 230000;
            case WEAVIATE -> 100000;
            // we can't increase this value, since there is a get/query limit
            // see https://github.com/milvus-io/milvus/issues/19007 and https://github.com/milvus-io/milvus/issues/5115
            case MILVUS -> 16384;
            default -> 10000;
        };
    }

    public static List<Object> getFakeIds(List<Map<String, Object>> data) {
        return data.stream().map(x -> x.get("id")).toList();
    }
}
