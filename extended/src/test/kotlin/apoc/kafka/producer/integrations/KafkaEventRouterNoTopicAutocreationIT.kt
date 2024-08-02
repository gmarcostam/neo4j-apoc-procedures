package apoc.kafka.producer.integrations

import apoc.kafka.extensions.execute
import apoc.kafka.support.KafkaTestUtils
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import kotlin.test.assertEquals

@Suppress("UNCHECKED_CAST", "DEPRECATION")
class KafkaEventRouterNoTopicAutocreationIT: KafkaEventRouterBaseTSE() {

    @Test
    fun `should start even with no topic created`() {
        // when
        db = createDbWithKafkaConfigs(
            "kafka.bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers,
            "streams.source.topic.nodes.personNotDefined" to "Person{*}"
        )

        // then
        val count = db.execute("MATCH (n) RETURN COUNT(n) AS count") {
            it.columnAs<Long>("count").next()
        }
        assertEquals(0L, count)
    }

    @Test
    fun `should insert data without hanging`() = runBlocking {
        // given
        val personTopic = "person"
        val customerTopic = "customer"
        val neo4jTopic = "neo4j"
        val expectedTopics = listOf(personTopic, customerTopic, neo4jTopic)

        db = createDbWithKafkaConfigs(
            "kafka.bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers,
            "streams.source.topic.nodes.$personTopic" to "Person{*}",
            "streams.source.topic.nodes.$customerTopic" to "Customer{*}"
        )

        // we create a new node an check that the source plugin is working
        db.execute("CREATE (p:Person{id: 1})")
//        val config = KafkaConfiguration(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
        val consumer = KafkaTestUtils.createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
        consumer.subscribe(expectedTopics)
        // the consumer consumes the message from the topic
        consumer.use {
            val records = it.poll(5000)
            assertEquals(1, records.count())
        }

        // when
        val waitFor = 10000L
        withTimeout(waitFor) { // n.b. the default value for `max.block.ms` is 60 seconds so if exceeds `waitFor` throws a CancellationException
            async { db.execute("CREATE (p:Customer{id: 2})") }.await()
        }

        // then
        val count = db.execute("MATCH (n) RETURN COUNT(n) AS count") { it.columnAs<Long>("count").next() }
        assertEquals(2L, count)
    }

}