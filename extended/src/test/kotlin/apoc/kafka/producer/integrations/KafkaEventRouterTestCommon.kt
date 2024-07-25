package apoc.kafka.producer.integrations

import apoc.ApocConfig
import apoc.kafka.consumer.kafka.KafkaEventSinkBaseTSE
import apoc.kafka.extensions.execute
import apoc.kafka.support.Assert
import apoc.util.DbmsTestUtil
// import apoc.kafka.support.start
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.hamcrest.Matchers
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.rule.DbmsRule
import java.time.Duration
import java.util.concurrent.TimeUnit

object KafkaEventRouterTestCommon {

    private fun createTopic(topic: String, numTopics: Int, withCompact: Boolean) = run {
        val newTopic = NewTopic(topic, numTopics, 1)
        if (withCompact) {
            newTopic.configs(mapOf(
                    "cleanup.policy" to "compact",
                    "segment.ms" to "10",
                    "retention.ms" to "1",
                    "min.cleanable.dirty.ratio" to "0.01"))
        }
        newTopic
    }

    fun createTopic(topic: String, bootstrapServerMap: Map<String, Any>, numTopics: Int = 1, withCompact: Boolean = true) {
        AdminClient.create(bootstrapServerMap).use {
            val topics = listOf(createTopic(topic, numTopics, withCompact))
            it.createTopics(topics).all().get()
        }
    }

    fun assertTopicFilled(kafkaConsumer: KafkaConsumer<String, ByteArray>,
                          fromBeginning: Boolean = false,
                          timeout: Long = 30,
                          assertion: (ConsumerRecords<String, ByteArray>) -> Boolean = { it.count() == 1 }
        ) {
        Assert.assertEventually(ThrowingSupplier {
            if(fromBeginning) {
                kafkaConsumer.seekToBeginning(kafkaConsumer.assignment())
            }
            val records = kafkaConsumer.poll(Duration.ofSeconds(5))
            assertion(records)
        }, Matchers.equalTo(true), timeout, TimeUnit.SECONDS)
    }

//    fun extracted(vararg pairs: Pair<String, Any>) : GraphDatabaseService {
//        val mutableMapOf = mutableMapOf<String, Any>(
//            "streams.source.schema.polling.interval" to "0"
////            "bootstrap.servers" to KafkaEventSinkSuiteIT.kafka.bootstrapServers,
////            "streams.sink.enabled" to "true"
////            ,
////            "kafka.key.deserializer" to StringDeserializer::class.java.name
////            ,
////            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
////            "key.deserializer" to ByteArrayDeserializer::class.java.name
//
//            // todo - "kafka.key.deserializer" deve serializzare una stringa, oltre che un object!!
//
//            //                "streams.sink.topic.cypher.avro", "CREATE (p:Place{name: event.name, coordinates: event.coordinates, citizens: event.citizens})",
//            //                "kafka.key.deserializer", KafkaAvroDeserializer::class.java.name,
//            //                "kafka.value.deserializer", KafkaAvroDeserializer::class.java.name,
//            //                "kafka.schema.registry.url", KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl()
//        )
//
//        mutableMapOf.putAll(mapOf(*pairs))
//
//        KafkaEventSinkBaseTSE.dbms = DbmsTestUtil.startDbWithApocConfigs(
//            temporaryFolder,
//            mutableMapOf
//        )
//
////        db = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
////        TestUtil.registerProcedure(db, StreamsSinkProcedures::class.java, GlobalProcedures::class.java, PublishProcedures::class.java);
//        getDbServices()
//        return KafkaEventSinkBaseTSE.db
//    }

    fun initDbWithLogStrategy(db: GraphDatabaseService, strategy: String, otherConfigs: Map<String, String>? = null, constraints: List<String>? = null) {

//        ApocConfig.apocConfig().setProperty("streams.source.schema.polling.interval", "0")
//        ApocConfig.apocConfig().setProperty("kafka.streams.log.compaction.strategy", strategy)
//
//        otherConfigs?.forEach { (k, v) -> ApocConfig.apocConfig().setProperty(k, v) }
        // db.start()
        constraints?.forEach { db.execute(it) }
    }
}
