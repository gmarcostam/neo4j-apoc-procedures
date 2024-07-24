package apoc.load;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.FileWriter;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class LoadGephiTest {
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        TestUtil.registerProcedure(db, LoadGephi.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testLoadGephi() {
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
    public void testImportGraphML() throws Exception {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        final String file = ClassLoader.getSystemResource("gexf/data.gexf").toString();
        TestUtil.testCall(
                db, "CALL apoc.import.gexf($file,{readLabels:true})", map("file", file), (r) -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(3L, r.get("relationships"));
                });

        TestUtil.testCall(
                db,
                "MATCH (c) RETURN COUNT(c) AS c",
                null,
                (r) -> assertEquals(4L, r.get("c")));

        TestUtil.testCall(
                db,
                "MATCH (n) RETURN n as n",
                null,
                (r) -> assertEquals(1L, r.get("n")));
    }


}
