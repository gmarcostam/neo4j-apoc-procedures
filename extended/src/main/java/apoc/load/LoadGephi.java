package apoc.load;

import apoc.Extended;
import apoc.Pools;
import apoc.export.graphml.XmlGraphMLReader;
import apoc.export.util.CountingInputStream;
import apoc.export.util.CountingReader;
import apoc.export.util.ExportConfig;
import apoc.export.util.ProgressReporter;
import apoc.result.MapResult;
import apoc.result.ProgressInfo;
import apoc.util.CompressionAlgo;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.CompressionConfig.COMPRESSION;

@Extended
public class LoadGephi {

    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Pools pools;

    @Procedure
    @Description("apoc.load.gexf")
    public Stream<MapResult> gexf(
            @Name("urlOrBinary") Object urlOrBinary,
            @Name(value = "path", defaultValue = "/") String path,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config,
            @Name(value = "simple", defaultValue = "false") boolean simpleMode
    ) throws Exception {
        return xmlXpathToMapResult(urlOrBinary, simpleMode, path, config);
    }

    @Procedure(name = "apoc.import.gexf", mode = Mode.WRITE)
    @Description("Imports a graph from the provided GraphML file.")
    public Stream<ProgressInfo> importGexf(
            @Name("urlOrBinaryFile") Object urlOrBinaryFile, @Name("config") Map<String, Object> config) {
        ProgressInfo result = Util.inThread(pools, () -> {
            ExportConfig exportConfig = new ExportConfig(config);
            String file = null;
            String source = "binary";
            if (urlOrBinaryFile instanceof String) {
                file = (String) urlOrBinaryFile;
                source = "file";
            }
            ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(file, source, "graphml"));
            XmlGraphMLReader graphMLReader = new XmlGraphMLReader(db)
                    .reporter(reporter)
                    .batchSize(exportConfig.getBatchSize())
                    .relType(exportConfig.defaultRelationshipType())
                    .source(exportConfig.getSource())
                    .target(exportConfig.getTarget())
                    .nodeLabels(exportConfig.readLabels());

            if (exportConfig.storeNodeIds()) graphMLReader.storeNodeIds();

            try (CountingReader reader =
                         FileUtils.readerFor(urlOrBinaryFile, exportConfig.getCompressionAlgo(), urlAccessChecker)) {
                graphMLReader.parseXML(reader, terminationGuard);
            }

            return reporter.getTotal();
        });
        return Stream.of(result);
    }

    private Stream<MapResult> xmlXpathToMapResult(
            Object urlOrBinary, boolean simpleMode, String path, Map<String, Object> config) throws Exception {
        if (config == null) config = Collections.emptyMap();
        boolean failOnError = (boolean) config.getOrDefault("failOnError", true);
        try {
            Map<String, Object> headers = (Map) config.getOrDefault("headers", Collections.emptyMap());
            CountingInputStream is = FileUtils.inputStreamFor(
                    urlOrBinary,
                    headers,
                    null,
                    (String) config.getOrDefault(COMPRESSION, CompressionAlgo.NONE.name()),
                    urlAccessChecker);
            return parse(is, simpleMode, path, failOnError);
        } catch (Exception e) {
            if (!failOnError) return Stream.of(new MapResult(Collections.emptyMap()));
            else throw e;
        }
    }

    private Stream<MapResult> parse(InputStream data, boolean simpleMode, String path, boolean failOnError)
            throws Exception {
        List<MapResult> result = new ArrayList<>();
        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            documentBuilderFactory.setNamespaceAware(true);
            documentBuilderFactory.setIgnoringElementContentWhitespace(true);
            documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            documentBuilder.setEntityResolver((publicId, systemId) -> new InputSource(new StringReader("")));

            Document doc = documentBuilder.parse(data);
            XPathFactory xPathFactory = XPathFactory.newInstance();

            XPath xPath = xPathFactory.newXPath();

            path = StringUtils.isEmpty(path) ? "/" : path;
            XPathExpression xPathExpression = xPath.compile(path);
            NodeList nodeList = (NodeList) xPathExpression.evaluate(doc, XPathConstants.NODESET);

            for (int i = 0; i < nodeList.getLength(); i++) {
                final Deque<Map<String, Object>> stack = new LinkedList<>();

                handleNode(stack, nodeList.item(i), simpleMode);
                for (int index = 0; index < stack.size(); index++) {
                    result.add(new MapResult(stack.pollFirst()));
                }
            }
        } catch (FileNotFoundException e) {
            if (!failOnError) return Stream.of(new MapResult(Collections.emptyMap()));
            else throw e;
        } catch (Exception e) {
            if (!failOnError) return Stream.of(new MapResult(Collections.emptyMap()));
            else if (e instanceof SAXParseException && e.getMessage().contains("DOCTYPE is disallowed"))
                throw generateXmlDoctypeException();
            else throw e;
        }
        return result.stream();
    }

    private RuntimeException generateXmlDoctypeException() {
        throw new RuntimeException("XML documents with a DOCTYPE are not allowed.");
    }

    /**
     * Collects type and attributes for the node
     *
     * @param node
     * @param elementMap
     */
    private void handleTypeAndAttributes(Node node, Map<String, Object> elementMap) {
        // Set type
        if (node.getLocalName() != null) {
            elementMap.put("_type", node.getLocalName());
        }

        // Set the attributes
        if (node.getAttributes() != null) {
            NamedNodeMap attributeMap = node.getAttributes();
            for (int i = 0; i < attributeMap.getLength(); i++) {
                Node attribute = attributeMap.item(i);
                elementMap.put(attribute.getNodeName(), attribute.getNodeValue());
            }
        }
    }

    private void handleNode(Deque<Map<String, Object>> stack, Node node, boolean simpleMode) {
        terminationGuard.check();

        // Handle document node
        if (node.getNodeType() == Node.DOCUMENT_NODE) {
            NodeList children = node.getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                if (children.item(i).getLocalName() != null) {
                    handleNode(stack, children.item(i), simpleMode);
                    return;
                }
            }
        }

        Map<String, Object> elementMap = new LinkedHashMap<>();
        handleTypeAndAttributes(node, elementMap);

        // Set children
        NodeList children = node.getChildNodes();
        int count = 0;
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);

            // This is to deal with text between xml tags for example new line characters
            if (child.getNodeType() != Node.TEXT_NODE && child.getNodeType() != Node.CDATA_SECTION_NODE) {
                handleNode(stack, child, simpleMode);
                count++;
            } else {
                // Deal with text nodes
                handleTextNode(child, elementMap);
            }
        }

        if (children.getLength() > 0) {
            if (!stack.isEmpty()) {
                List<Object> nodeChildren = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    nodeChildren.add(stack.pollLast());
                }
                String key = simpleMode ? "_" + node.getLocalName() : "_children";
                Collections.reverse(nodeChildren);
                if (nodeChildren.size() > 0) {
                    // Before adding the children we need to handle mixed text
                    Object text = elementMap.get("_text");
                    if (text instanceof List) {
                        for (Object element : (List) text) {
                            nodeChildren.add(element);
                        }
                        elementMap.remove("_text");
                    }

                    elementMap.put(key, nodeChildren);
                }
            }
        }

        if (!elementMap.isEmpty()) {
            stack.addLast(elementMap);
        }
    }

    /**
     * Handle TEXT nodes and CDATA nodes
     *
     * @param node
     * @param elementMap
     */
    private void handleTextNode(Node node, Map<String, Object> elementMap) {
        Object text = "";
        int nodeType = node.getNodeType();
        switch (nodeType) {
            case Node.TEXT_NODE:
                text = normalizeText(node.getNodeValue());
                break;
            case Node.CDATA_SECTION_NODE:
                text = normalizeText(((CharacterData) node).getData());
                break;
            default:
                break;
        }

        // If the text is valid ...
        if (!StringUtils.isEmpty(text.toString())) {
            // We check if we have already collected some text previously
            Object previousText = elementMap.get("_text");
            if (previousText != null) {
                // If we just have a "_text" key than we need to collect to a List
                text = Arrays.asList(previousText.toString(), text);
            }
            elementMap.put("_text", text);
        }
    }

    /**
     * Remove trailing whitespaces and new line characters
     *
     * @param text
     * @return
     */
    private String normalizeText(String text) {
        String[] tokens = StringUtils.split(text, "\n");
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].trim();
        }

        return StringUtils.join(tokens, " ").trim();
    }
}
