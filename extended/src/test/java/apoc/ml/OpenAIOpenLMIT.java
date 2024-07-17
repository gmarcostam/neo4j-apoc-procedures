package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.ml.MLUtil.*;
import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.OpenAI.PATH_CONF_KEY;
import static apoc.ml.OpenAITestResultUtils.COMPLETION_QUERY;
import static apoc.ml.OpenAITestResultUtils.COMPLETION_QUERY_EXTENDED;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OpenLM-like tests for Cohere and HuggingFace, see here: https://github.com/r2d4/openlm
 * 
 * NB: It works only for `Completion` API, as described in the README.md:
 * https://github.com/r2d4/openlm/blob/main/README.md?plain=1#L36
 */
public class OpenAIOpenLMIT {
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, OpenAI.class);

    }

    @Test
    public void completionWithAnthropic() {
        String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

        Map<String, Object> conf = Map.of(
                ENDPOINT_CONF_KEY, "https://api.anthropic.com/v1/messages",
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
                MAX_TOKENS, 1024
        );
        testCall(db, COMPLETION_QUERY_EXTENDED,
                Map.of("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var contentList = (List<Map<String, Object>>) result.get("content");
                    Map<String, Object> content = contentList.get(0);
                    String generatedText = (String) content.get("text");
                    assertTrue(generatedText.toLowerCase().contains("blue"),
                            "Actual generatedText is " + generatedText);
                });
    }

    @Test
    public void completionWithAnthropicNonDefaultModel() {
        String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

        String modelId = "claude-3-haiku-20240307";
        Map<String, Object> conf = Map.of(
                ENDPOINT_CONF_KEY, "https://api.anthropic.com/v1/messages",
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
                MODEL_CONF_KEY, modelId,
                MAX_TOKENS, 1024
        );
        testCall(db, COMPLETION_QUERY_EXTENDED,
                Map.of("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var contentList = (List<Map<String, Object>>) result.get("content");
                    Map<String, Object> content = contentList.get(0);
                    String generatedText = (String) content.get("text");
                    assertTrue(generatedText.toLowerCase().contains("blue"),
                            "Actual generatedText is " + generatedText);
                });
    }

    @Test
    public void completionWithAnthropicUnkwnownModel() {
        String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

        String modelId = "unknown";
        Map<String, Object> conf = Map.of(
                ENDPOINT_CONF_KEY, "https://api.anthropic.com/v1/messages",
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
                MODEL_CONF_KEY, modelId,
                MAX_TOKENS, 1024
        );

        assertFails(
                db,
                COMPLETION_QUERY_EXTENDED,
                Map.of("conf", conf, "apiKey", anthropicApiKey),
                "Failed to invoke procedure `apoc.ml.openai.completion`: Caused by: java.io.FileNotFoundException: https://api.anthropic.com/v1/messages"
        );
    }

@Test
public void completionWithAnthropicSmallTokenSize() {
    String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
    Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

    Map<String, Object> conf = Map.of(
            ENDPOINT_CONF_KEY, "https://api.anthropic.com/v1/messages",
            API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
            MAX_TOKENS, 1
    );

    testCall(db, COMPLETION_QUERY_EXTENDED,
            Map.of("conf", conf, "apiKey", anthropicApiKey),
            (row) -> {
                var result = (Map<String,Object>) row.get("value");
                var contentList = (List<Map<String, Object>>) result.get("content");
                Map<String, Object> content = contentList.get(0);
                String generatedText = (String) content.get("text");
                String[] wordCount = generatedText.toLowerCase().split(" ");
                assertEquals(1, wordCount.length);
            });
}

@Test
public void completionWithAnthropicCustomVersion() {
    String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
    Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

    Map<String, Object> conf = Map.of(
            ENDPOINT_CONF_KEY, "https://api.anthropic.com/v1/messages",
            API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
            MAX_TOKENS, 1024,
            ANTHROPIC_VERSION, "2023-06-01"
    );
    testCall(db, COMPLETION_QUERY_EXTENDED,
            Map.of("conf", conf, "apiKey", anthropicApiKey),
            (row) -> {
                var result = (Map<String,Object>) row.get("value");
                var contentList = (List<Map<String, Object>>) result.get("content");
                Map<String, Object> content = contentList.get(0);
                String generatedText = (String) content.get("text");
                assertTrue(generatedText.toLowerCase().contains("blue"),
                        "Actual generatedText is " + generatedText);
            });
}

@Test
public void completionWithAnthropicWrongVersion() {
    String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
    Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

    Map<String, Object> conf = Map.of(
            ENDPOINT_CONF_KEY, "https://api.anthropic.com/v1/messages",
            API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
            MAX_TOKENS, 1024,
            ANTHROPIC_VERSION, "ajeje"
    );

    assertFails(
            db,
            COMPLETION_QUERY_EXTENDED,
            Map.of("conf", conf, "apiKey", anthropicApiKey),
            "Server returned HTTP response code: 400 for URL: https://api.anthropic.com/v1/messages"
    );
}

@Ignore
@Test
public void completionWithAnthropicStream() {
    String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
    Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

    Map<String, Object> conf = Map.of(
            ENDPOINT_CONF_KEY, "https://api.anthropic.com/v1/messages",
            API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
            MAX_TOKENS, 256,
            "stream", true
    );
    testCall(db, COMPLETION_QUERY_EXTENDED,
            Map.of("conf", conf, "apiKey", anthropicApiKey),
            (row) -> {
                var result = (Map<String,Object>) row.get("value");
                var contentList = (List<Map<String, Object>>) result.get("content");
                Map<String, Object> content = contentList.get(0);
                String generatedText = (String) content.get("text");
                assertTrue(generatedText.toLowerCase().contains("blue"),
                        "Actual generatedText is " + generatedText);
            });
}

    /**
     * Request converter similar to: https://github.com/r2d4/openlm/blob/main/openlm/llm/huggingface.py
     */
    @Test
    public void completionWithHuggingFace() {
        String huggingFaceApiKey = System.getenv("HF_API_TOKEN");
        Assume.assumeNotNull("No HF_API_TOKEN environment configured", huggingFaceApiKey);
        
        String modelId = "gpt2";
        Map<String, String> conf = Map.of(ENDPOINT_CONF_KEY, "https://api-inference.huggingface.co/models/" + modelId,
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.HUGGINGFACE.name(),
                PATH_CONF_KEY, "",
                MODEL_CONF_KEY, modelId
        );
        testCall(db, COMPLETION_QUERY,
                Map.of("conf", conf, "apiKey", huggingFaceApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    String generatedText = (String) result.get("generated_text");
                    assertTrue(generatedText.toLowerCase().contains("blue"),
                            "Actual generatedText is " + generatedText);
                });
    }

    /**
     * Request converter similar to: https://github.com/r2d4/openlm/blob/main/openlm/llm/cohere.py
     */
    @Test
    public void completionWithCohere() {
        String cohereApiKey = System.getenv("COHERE_API_TOKEN");
        Assume.assumeNotNull("No COHERE_API_TOKEN environment configured", cohereApiKey);
        
        String modelId = "command";
        Map<String, String> conf = Map.of(ENDPOINT_CONF_KEY, "https://api.cohere.ai/v1/generate",
                PATH_CONF_KEY, "",
                MODEL_CONF_KEY, modelId
        );
        
        testCall(db, COMPLETION_QUERY,
                Map.of("conf", conf, "apiKey", cohereApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    Map meta = (Map) result.get("meta");
                    assertEquals(Set.of("warnings", "billed_units", "api_version"), meta.keySet());

                    List<Map> generations = (List<Map>) result.get("generations");
                    assertEquals(1, generations.size());
                    assertEquals(Set.of("finish_reason", "id", "text"), generations.get(0).keySet());
                    
                    assertTrue(result.get("id") instanceof String);
                    assertTrue(result.get("prompt") instanceof String);
                });
    }
}