package apoc.load;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class GexfTest {
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        TestUtil.registerProcedure(db, Gexf.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testLoadGexf() {
        final String file = ClassLoader.getSystemResource("gexf/data.gexf").toString();
        testCall(
                db,
                "CALL apoc.load.gexf($file)",
                Map.of("file", file),
                (row) -> {
                    Map<String, Object> value = (Map) row.get("value");
                    assertEquals("gexf", value.get("_type"));
                    assertEquals("http://gexf.net/1.3 http://gexf.net/1.3/gexf.xsd", value.get("xsi:schemaLocation"));
                    assertEquals("http://gexf.net/1.3", value.get("xmlns"));
                });
    }

    @Test
    public void testImportGexf() throws Exception {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        final String file = ClassLoader.getSystemResource("gexf/data.gexf").toString();
        TestUtil.testCall(
                db,
                "CALL apoc.import.gexf($file,{readLabels:true})",
                map("file", file),
                (r) -> {
                    assertEquals("gexf", r.get("format"));
                    assertEquals(5L, r.get("nodes"));
                    assertEquals(6L, r.get("relationships"));
                });

        TestUtil.testCallCount(db, "MATCH (n) RETURN n",5);

        TestUtil.testResult(db, "MATCH (n:Gephi) RETURN properties(n) as props", r -> {
            ResourceIterator<Map> propsIterator = r.columnAs("props");
            Map props = propsIterator.next();
            assertEquals("http://gephi.org", props.get("0"));
            assertEquals("1", props.get("1"));

            props = propsIterator.next();
            assertEquals("http://test.gephi.org", props.get("0"));
        });

        TestUtil.testResult(
                db,
                "MATCH ()-[r]->() RETURN DISTINCT properties(r) as props, type(r) as type",
                r -> {
            ResourceIterator<Map> propsIterator = r.columnAs("props");
            Map props = propsIterator.next();
            assertEquals("1.5", props.get("score"));

            ResourceIterator<String> typeIterator = r.columnAs("type");
            String type = typeIterator.next();
            assertEquals("KNOWS", type);
        });
    }
}
