package apoc.ml;

import apoc.util.TestUtil;
import com.unboundid.ldap.sdk.persist.ObjectEncoder;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.ml.MLUtil.*;
import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.OpenAI.PATH_CONF_KEY;
import static apoc.ml.OpenAITestResultUtils.CHAT_COMPLETION_QUERY;
import static apoc.ml.OpenAITestResultUtils.CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM;
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
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name()
        );
        testCall(db, COMPLETION_QUERY_EXTENDED,
                Map.of("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var completion = (String) result.get("completion");
                    assertTrue(completion.toLowerCase().contains("blue"),
                            "Actual generatedText is " + completion);
                });
    }

    @Test
    public void chatWithAnthropic() {
        String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

        Map<String, Object> conf = Map.of(
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name()
        );
        testCall(db, CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
                Map.of("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var contentList = (List<Map<String, Object>>) result.get("content");
                    Map<String, Object> content = contentList.get(0);
                    String generatedText = (String) content.get("text");
                    assertTrue(generatedText.toLowerCase().contains("earth"),
                            "Actual generatedText is " + generatedText);
                });
    }

    @Test
    public void chatWithImageAnthropic() throws IOException {
        String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

        String path = Thread.currentThread().getContextClassLoader().getResource("tarallo.jpeg").getPath();
        byte[] fileContent = FileUtils.readFileToByteArray(new File(path));
        String base64Image = Base64.getEncoder().encodeToString(fileContent);

        List<Map<String, ?>> parts = List.of(
                Map.of(
                        "type", "image",
                        "source", Map.of(
                                "type", "base64",
                                "media_type", "image/jpeg",
                                "data", base64Image
                        )
                ),
                Map.of(
                        "type", "text",
                        "text", "What is in the above image?"
                )
        );

        List<Map<String, Object>> messages = List.of(Map.of(
                "role", "user",
                "content", parts
        ));

        String query = "CALL apoc.ml.openai.chat($content, $apiKey, $conf)";

        Map<String, Object> conf = Map.of(
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name()
        );
        testCall(db, query,
                Map.of( "content", messages,"conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var contentList = (List<Map<String, Object>>) result.get("content");
                    Map<String, Object> content = contentList.get(0);
                    String generatedText = (String) content.get("text");
                    Assertions.assertThat(generatedText).containsAnyOf("tarallo", "bagel");
                });
    }

    @Test
    public void completionWithAnthropicNonDefaultModel() {
        String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

        String modelId = "claude-3-haiku-20240307";
        Map<String, Object> conf = Map.of(
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
                MODEL_CONF_KEY, modelId
        );
        testCall(db, CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
                Map.of("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var contentList = (List<Map<String, Object>>) result.get("content");
                    Map<String, Object> content = contentList.get(0);
                    String generatedText = (String) content.get("text");
                    assertTrue(generatedText.toLowerCase().contains("earth"),
                            "Actual generatedText is " + generatedText);
                });
    }

    @Test
    public void completionWithAnthropicUnkwnownModel() {
        String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

        String modelId = "unknown";
        Map<String, Object> conf = Map.of(
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
                MODEL_CONF_KEY, modelId
        );

        assertFails(
                db,
                CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
                Map.of("conf", conf, "apiKey", anthropicApiKey),
                "Caused by: java.io.FileNotFoundException: https://api.anthropic.com/v1/messages"
        );
    }

    @Test
    public void completionWithAnthropicSmallTokenSize() {
        String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

        Map<String, Object> conf = Map.of(
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
                MAX_TOKENS, 1
        );

        testCall(db, CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
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
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
                ANTHROPIC_VERSION, "2023-06-01"
        );
        testCall(db, CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
                Map.of("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var contentList = (List<Map<String, Object>>) result.get("content");
                    Map<String, Object> content = contentList.get(0);
                    String generatedText = (String) content.get("text");
                    assertTrue(generatedText.toLowerCase().contains("earth"),
                            "Actual generatedText is " + generatedText);
                });
    }

    @Test
    public void completionWithAnthropicWrongVersion() {
        String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

        Map<String, Object> conf = Map.of(
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
                ANTHROPIC_VERSION, "ajeje"
        );

        assertFails(
                db,
                COMPLETION_QUERY_EXTENDED,
                Map.of("conf", conf, "apiKey", anthropicApiKey),
                "Server returned HTTP response code: 400 for URL"
        );
    }

    @Test
    public void chatWithAnthropicWrongVersion() {
        String anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);

        Map<String, Object> conf = Map.of(
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANTHROPIC.name(),
                ANTHROPIC_VERSION, "ajeje"
        );

        assertFails(
                db,
                CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
                Map.of("conf", conf, "apiKey", anthropicApiKey),
                "Server returned HTTP response code: 400 for URL"
        );
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