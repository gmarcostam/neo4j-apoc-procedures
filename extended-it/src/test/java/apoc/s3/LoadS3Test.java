package apoc.s3;

import apoc.load.LoadCsv;
import apoc.load.LoadDirectory;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.load.xls.LoadXls;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.xml.XmlTestUtils;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang3.tuple.Pair;
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
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadS3Test extends S3BaseTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class, LoadDirectory.class, LoadJson.class, LoadXls.class, Xml.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        putFolderToS3();
    }

    @AfterAll
    public void tearDownAll() {
        db.shutdown();
    }

    @Test
    public void testLoadCsvS3() {
        String url = s3Container.putFile(EXTENDED_PATH +  "src/test/resources/test.csv");
        url = removeRegionFromUrl(url);

        testResult(db, "CALL apoc.load.csv($url,{failOnError:false})", map("url", url), (r) -> {
            assertRow(r, "Selma", "8", 0L);
            assertRow(r, "Rana", "11", 1L);
            assertRow(r, "Selina", "18", 2L);
            assertFalse(r.hasNext());
        });
    }

    @Test public void testLoadJsonS3() {
        String url = s3Container.putFile(EXTENDED_PATH +  "src/test/resources/map.json");
        url = removeRegionFromUrl(url);

        testCall(db, "CALL apoc.load.json($url,'')",map("url", url),
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }

    @Test public void testLoadXmlS3() {
        String url = s3Container.putFile(EXTENDED_PATH +  "src/test/resources/xml/books.xml");
        url = removeRegionFromUrl(url);

        testCall(db, "CALL apoc.load.xml($url,'/catalog/book[title=\"Maeve Ascendant\"]/.',{failOnError:false}) yield value as result", Util.map("url", url), (r) -> {
            Object value = Iterables.single(r.values());
            Assert.assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
        });
    }

    @Test public void testLoadDirectoryS3() {
        String url = s3Container.getUrl("test_folder");
        url = removeRegionFromUrl(url);

// Da usare solo per url https
//        URI uri = URI.create(url);
//        AmazonS3URI s3Uri = new AmazonS3URI(uri);

        // Esempio di parse s3 url e recupero lista file da una cartella s3
//        Pair<String, String> s3Uri = s3Container.parseS3URI(url);
//
//        List<S3ObjectSummary> s3FolderList = s3Container.listBucket(s3Uri.getLeft(), s3Uri.getRight());

        String query = """
                CALL apoc.load.directory('*.csv', $url)
                YIELD value WITH value as url ORDER BY url DESC
                CALL apoc.load.csv(url, {results:['map']}) YIELD map RETURN map
                """;

        testCall(db, query, Util.map("url", url), (r) -> {
            System.out.println(r.get("value"));
//            Object value = Iterables.single(r.values());
//            Assert.assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
            System.out.println("END");
        });
    }

    @Test public void testLoadXlsFromS3() {
        String loadTest = Thread.currentThread().getContextClassLoader().getResource("load_test.xlsx").getPath();
        String url = s3Container.putFile(loadTest);
        url = removeRegionFromUrl(url);

        testResult(db, "CALL apoc.load.xls($url,'Full',{mapping:{Integer:{type:'int'}, Array:{type:'int',array:true,arraySep:';'}}})", map("url",url), // 'file:load_test.xlsx'
                (r) -> {
                    assertXlsRow(r,0L,"String","Test","Boolean",true,"Integer",2L,"Float",1.5d,"Array",asList(1L,2L,3L));
                    assertFalse("Should not have another row",r.hasNext());
                });
    }

    private String removeRegionFromUrl(String url) {
        return url.replace(s3Container.getEndpointConfiguration().getSigningRegion() + ".", "");
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
