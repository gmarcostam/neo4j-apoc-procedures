package apoc.s3;

import apoc.load.LoadCsv;
import apoc.load.LoadDirectory;
import apoc.load.LoadHtml;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.load.xls.LoadXls;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.xml.XmlTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.ExtendedITUtil.EXTENDED_PATH;
import static apoc.util.MapUtil.map;
import static apoc.util.S3Util.putToS3AndGetUrl;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LoadS3Test extends S3BaseTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class, LoadDirectory.class, LoadJson.class, LoadHtml.class, LoadXls.class, Xml.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        putFolderToS3();
    }

    @AfterAll
    public void tearDownAll() {
        db.shutdown();
    }

    @Test
    public void testLoadCsv() {
        String url = putToS3AndGetUrl(s3Container, EXTENDED_PATH + "src/test/resources/test.csv");
        testResult(db, "CALL apoc.load.csv($url,{failOnError:false})", map("url", url), (r) -> {
            assertRow(r, "Selma", "8", 0L);
            assertRow(r, "Rana", "11", 1L);
            assertRow(r, "Selina", "18", 2L);
            assertFalse(r.hasNext());
        });
    }

    @Test public void testLoadJson() {
        String url = putToS3AndGetUrl(s3Container, EXTENDED_PATH + "src/test/resources/map.json");
        testCall(db, "CALL apoc.load.json($url,'')",map("url", url),
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }

    @Test public void testLoadXml() {
        String url = putToS3AndGetUrl(s3Container, EXTENDED_PATH + "src/test/resources/xml/books.xml");
        testCall(db, "CALL apoc.load.xml($url,'/catalog/book[title=\"Maeve Ascendant\"]/.',{failOnError:false}) yield value as result", Util.map("url", url), (r) -> {
            Object value = Iterables.single(r.values());
            Assert.assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
        });
    }

    @Test public void testLoadXls() {
        String url = putToS3AndGetUrl(s3Container, EXTENDED_PATH + "src/test/resources/load_test.xlsx");
        testResult(db, "CALL apoc.load.xls($url,'Full',{mapping:{Integer:{type:'int'}, Array:{type:'int',array:true,arraySep:';'}}})", map("url",url), // 'file:load_test.xlsx'
                (r) -> {
                    assertXlsRow(r,0L,"String","Test","Boolean",true,"Integer",2L,"Float",1.5d,"Array",asList(1L,2L,3L));
                    assertFalse("Should not have another row",r.hasNext());
                });
    }

    @Test
    public void testLoadHtml() {
        String url = putToS3AndGetUrl(s3Container, EXTENDED_PATH + "src/test/resources/wikipedia.html");

        Map<String, Object> query = map("links", "a[href]");

        testCall(db, "CALL apoc.load.html($url,$query)",
                map("url", url, "query", query),
                row -> {
                    final List<Map<String, Object>> actual = (List) ((Map) row.get("value")).get("links");
                    assertEquals(106, actual.size());
                    assertTrue(actual.stream().allMatch(i -> i.get("tagName").equals("a")));
                });
    }

    private void putFolderToS3() {
        StringBuilder csv= new StringBuilder(); // Faster
        csv.append("name,age\r\n");
        csv.append("Bonzo,20\r\n");
        csv.append("Oronzo,45\r\n");
        byte[] data = csv.toString().getBytes(StandardCharsets.UTF_8);

        s3Container.putObjectToS3("test_folder/test.csv", data);

        csv = new StringBuilder();
        csv.append("name,age\r\n");
        csv.append("Bobby,18\r\n");
        csv.append("Maruccio,90\r\n");
        data = csv.toString().getBytes(StandardCharsets.UTF_8);

        s3Container.putObjectToS3("test_folder/test_1.csv", data);
    }

    static void assertXlsRow(Result r, long lineNo, Object...data) {
        Map<String, Object> row = r.next();
        Map<String, Object> map = map(data);
        assertEquals(map, row.get("map"));
        Map<Object, Object> stringMap = new LinkedHashMap<>(map.size());
        map.forEach((k,v) -> stringMap.put(k,v == null ? null : v.toString()));
        assertEquals(new ArrayList<>(map.values()), row.get("list"));
        assertEquals(lineNo, row.get("lineNo"));
    }

}
