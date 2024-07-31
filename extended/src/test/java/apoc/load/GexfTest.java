package apoc.load;

import apoc.util.TestUtil;
import apoc.util.collection.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;
import java.util.Set;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.ExtendedTestUtil.assertMapEquals;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
        final String file = ClassLoader.getSystemResource("gexf/load.gexf").toString();
        testCall(
                db,
                "CALL apoc.load.gexf($file)",
                Map.of("file", file),
                (row) -> {
                    Map<String, Object> value = (Map) row.get("value");
                    String expected = "{_type=gexf, _children=[{_type=graph, defaultedgetype=directed, _children=[{_type=nodes, _children=[{_type=node, _children=[{_type=attvalues, _children=[{_type=attvalue, for=0, value=http://gephi.org}]}], foo=bar}]}]}], version=1.2}";
                    assertEquals(expected, value.toString());
                });
    }

    @Test
    public void testImportGexf() {
        final String file = ClassLoader.getSystemResource("gexf/data.gexf").toString();
        TestUtil.testCall(
                db,
                "CALL apoc.import.gexf($file,{readLabels:true})",
                map("file", file),
                (r) -> {
                    assertEquals("gexf", r.get("format"));
                    assertEquals(5L, r.get("nodes"));
                    assertEquals(8L, r.get("relationships"));
                });

        TestUtil.testCallCount(db, "MATCH (n) RETURN n",5);

        TestUtil.testResult(db, "MATCH (n:Gephi) RETURN properties(n) as props", r -> {
            ResourceIterator<Map> propsIterator = r.columnAs("props");
            Map props = propsIterator.next();
            assertEquals("http://gephi.org", props.get("0"));
            assertEquals(1.0f, props.get("1"));

            props = propsIterator.next();
            assertEquals("http://test.gephi.org", props.get("0"));
        });

        TestUtil.testResult(db, "MATCH (n:BarabasiLab) RETURN properties(n) as props", r -> {
            ResourceIterator<Map> propsIterator = r.columnAs("props");
            Map props = propsIterator.next();
            assertEquals("http://barabasilab.com", props.get("0"));
            assertEquals(1.0f, props.get("1"));
        });

        TestUtil.testResult(
                db,
                "MATCH ()-[rel]->() RETURN rel ORDER BY rel.score",
                r -> {
                    final ResourceIterator<Relationship> rels = r.columnAs("rel");

                    Relationship rel = rels.next();
                    Node startNode = rel.getStartNode();
                    Node endNode = rel.getEndNode();

                    Map<String, Object> expectedNode = Map.of(
                            "0", "http://gephi.org",
                            "1", 1.0f,
                            "room", 10,
                            "price", Double.parseDouble("10.02"),
                            "projects", 300L,
                            "members", new String[] {"Altomare", "Sterpeto", "Lino"},
                            "pins", new boolean[]{true, false, true, false}
                    );

                    assertMapEquals(Map.of("score", 1.5f), rel.getAllProperties());
                    assertEquals(RelationshipType.withName("KNOWS"), rel.getType());
                    assertEquals(Set.of(Label.label("Gephi")), Iterables.asSet(startNode.getLabels()));
                    assertMapEquals(expectedNode, startNode.getAllProperties());
                    assertMapEquals(Map.of("0", "http://webatlas.fr", "1", 2.0f), endNode.getAllProperties());
                    assertEquals(Set.of(Label.label("Webatlas")), Iterables.asSet(endNode.getLabels()));

                    rel = rels.next();
                    startNode = rel.getStartNode();
                    endNode = rel.getEndNode();
                    assertMapEquals(Map.of("score", 2.0f, "foo", "bar"), rel.getAllProperties());
                    assertEquals(RelationshipType.withName("BAZ"), rel.getType());
                    assertEquals(Set.of(Label.label("Gephi")), Iterables.asSet(startNode.getLabels()));
                    assertMapEquals(expectedNode, endNode.getAllProperties());
                    assertEquals(Set.of(Label.label("Gephi")), Iterables.asSet(endNode.getLabels()));

                    rel = rels.next();
                    startNode = rel.getStartNode();
                    endNode = rel.getEndNode();

                    assertMapEquals(Map.of("score", 3f, "ajeje", "brazorf"), rel.getAllProperties());
                    assertEquals(RelationshipType.withName("HAS_TICKET"), rel.getType());
                    assertEquals(Set.of(Label.label("Gephi")), Iterables.asSet(startNode.getLabels()));
                    assertMapEquals(Map.of("0", "http://rtgi.fr", "1", 1.0f), endNode.getAllProperties());
                    assertEquals(Set.of(Label.label("RTGI")), Iterables.asSet(endNode.getLabels()));

                    rel = rels.next();
                    startNode = rel.getStartNode();
                    endNode = rel.getEndNode();

                    assertMapEquals(Map.of(), rel.getAllProperties());
                    assertEquals(RelationshipType.withName("KNOWS"), rel.getType());
                    assertEquals(Set.of(Label.label("Gephi")), Iterables.asSet(startNode.getLabels()));
                    assertMapEquals(Map.of("0", "http://rtgi.fr", "1", 1.0f), endNode.getAllProperties());
                    assertEquals(Set.of(Label.label("RTGI")), Iterables.asSet(endNode.getLabels()));

                    rel = rels.next();
                    startNode = rel.getStartNode();
                    endNode = rel.getEndNode();

                    assertMapEquals(Map.of(), rel.getAllProperties());
                    assertEquals(RelationshipType.withName("KNOWS"), rel.getType());
                    assertEquals(Set.of(Label.label("Webatlas")), Iterables.asSet(startNode.getLabels()));
                    assertMapEquals(expectedNode, endNode.getAllProperties());
                    assertEquals(Set.of(Label.label("Gephi")), Iterables.asSet(endNode.getLabels()));

                    rel = rels.next();
                    startNode = rel.getStartNode();
                    endNode = rel.getEndNode();

                    assertMapEquals(Map.of(), rel.getAllProperties());
                    assertEquals(RelationshipType.withName("KNOWS"), rel.getType());
                    assertEquals(Set.of(Label.label("RTGI")), Iterables.asSet(startNode.getLabels()));
                    assertMapEquals(Map.of("0", "http://webatlas.fr", "1", 2.0f), endNode.getAllProperties());
                    assertEquals(Set.of(Label.label("Webatlas")), Iterables.asSet(endNode.getLabels()));

                    rel = rels.next();
                    startNode = rel.getStartNode();
                    endNode = rel.getEndNode();

                    assertMapEquals(Map.of(), rel.getAllProperties());
                    assertEquals(RelationshipType.withName("KNOWS"), rel.getType());
                    assertEquals(Set.of(Label.label("Gephi")), Iterables.asSet(startNode.getLabels()));
                    assertMapEquals(Map.of("0", "http://barabasilab.com", "1", 1.0f, "2", false), endNode.getAllProperties());
                    assertEquals(Set.of(Label.label("Webatlas"), Label.label("BarabasiLab")), Iterables.asSet(endNode.getLabels()));

                    rel = rels.next();
                    startNode = rel.getStartNode();
                    endNode = rel.getEndNode();

                    assertMapEquals(Map.of(), rel.getAllProperties());
                    assertEquals(RelationshipType.withName("KNOWS"), rel.getType());
                    assertEquals(Set.of(Label.label("Gephi")), Iterables.asSet(startNode.getLabels()));
                    assertMapEquals(Map.of("0", "http://barabasilab.com", "1", 1.0f, "2", false), endNode.getAllProperties());
                    assertEquals(Set.of(Label.label("Webatlas"), Label.label("BarabasiLab")), Iterables.asSet(endNode.getLabels()));

                    assertFalse(rels.hasNext());
                }
        );
    }
}
