//package streams
//
//import apoc.kafka.consumer.StreamsEventSinkQueryExecution
//import apoc.kafka.consumer.StreamsSinkConfiguration
//import apoc.kafka.consumer.StreamsTopicService
//import apoc.kafka.consumer.kafka.KafkaSinkConfiguration
//import apoc.kafka.service.StreamsSinkEntity
//import apoc.kafka.service.StreamsStrategyStorage
//import apoc.kafka.service.TopicType
//import apoc.kafka.service.Topics
//import apoc.kafka.service.sink.strategy.CypherTemplateStrategy
//import apoc.kafka.service.sink.strategy.IngestionStrategy
//import apoc.kafka.support.shutdownSilently
//import apoc.kafka.support.start
//import org.junit.After
//import org.junit.Before
//import org.junit.Test
//import org.neo4j.kernel.internal.GraphDatabaseAPI
//import org.neo4j.logging.NullLog
//import org.neo4j.test.rule.DbmsRule
//import org.neo4j.test.rule.ImpermanentDbmsRule
//import kotlin.test.assertEquals
//
//class StreamsEventSinkQueryExecutionTest {
//    private lateinit var db: DbmsRule
//    private lateinit var streamsEventSinkQueryExecution: StreamsEventSinkQueryExecution
//
//    @Before
//    fun setUp() {
//        db = ImpermanentDbmsRule().start()
//        val kafkaConfig = KafkaSinkConfiguration(streamsSinkConfiguration = StreamsSinkConfiguration(topics = Topics(cypherTopics = mapOf("shouldWriteCypherQuery" to "MERGE (n:Label {id: event.id})\n" +
//                "    ON CREATE SET n += event.properties"))
//        )
//        )
//        val streamsTopicService = StreamsTopicService()
//        streamsTopicService.set(TopicType.CYPHER, kafkaConfig.streamsSinkConfiguration.topics.cypherTopics)
//        streamsEventSinkQueryExecution = StreamsEventSinkQueryExecution(db as GraphDatabaseAPI, NullLog.getInstance(),
//                object : StreamsStrategyStorage() {
//            override fun getTopicType(topic: String): TopicType? {
//                TODO("not implemented")
//            }
//
//            override fun getStrategy(topic: String): IngestionStrategy {
//                return CypherTemplateStrategy(streamsTopicService.getCypherTemplate(topic)!!)
//            }
//
//        })
//    }
//
//    @After
//    fun tearDown() {
//        db.shutdownSilently()
//    }
//
//    @Test
//    fun shouldWriteCypherQuery() {
//        // given
//        val first = mapOf("id" to "1", "properties" to mapOf("a" to 1))
//        val second = mapOf("id" to "2", "properties" to mapOf("a" to 1))
//
//        // when
//        streamsEventSinkQueryExecution.writeForTopic("shouldWriteCypherQuery", listOf(
//            StreamsSinkEntity(first, first),
//                StreamsSinkEntity(second, second)))
//
//        // then
//        db.executeTransactionally("MATCH (n:Label) RETURN count(n) AS count", emptyMap()) { it.columnAs<Long>("count").next() }
//                .let { assertEquals(2, it) }
//    }
//
//}