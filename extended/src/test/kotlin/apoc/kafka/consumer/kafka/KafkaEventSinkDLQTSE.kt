//package apoc.kafka.consumer.kafka
//
//import apoc.kafka.service.errors.ErrorService
//import apoc.kafka.support.Assert
//import apoc.kafka.support.KafkaTestUtils
//import apoc.kafka.support.setConfig
//import apoc.kafka.support.start
//import apoc.kafka.utils.JSONUtils
//import apoc.util.JsonUtil
//import org.apache.kafka.clients.producer.ProducerRecord
//import org.apache.kafka.common.serialization.ByteArrayDeserializer
//import org.hamcrest.Matchers
//import org.junit.Test
//import org.neo4j.function.ThrowingSupplier
//import java.util.*
//import java.util.concurrent.TimeUnit
//
//class KafkaEventSinkDLQTSE : KafkaEventSinkBaseTSE() {
//    @Test
//    fun `should send data to the DLQ because of QueryExecutionException`() {
//        val topic = UUID.randomUUID().toString()
//        val dlqTopic = UUID.randomUUID().toString()
//        db.setConfig("streams.sink.topic.cypher.$topic","MERGE (c:Customer {id: event.id})")
//        db.setConfig("streams.sink."+ErrorService.ErrorConfig.TOLERANCE, "all")
//        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_TOPIC, dlqTopic)
//        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADERS, "true")
//        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADER_PREFIX, "__streams.errors.")
//        db.start()
//        val data = mapOf("id" to null, "name" to "Andrea", "surname" to "Santurbano")
//
//        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
//        kafkaProducer.send(producerRecord).get()
//        val dlqConsumer = KafkaTestUtils.createConsumer<ByteArray, ByteArray>(
//                bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
//                schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl(),
//                keyDeserializer = ByteArrayDeserializer::class.java.name,
//                valueDeserializer = ByteArrayDeserializer::class.java.name,
//                topics = *arrayOf(dlqTopic))
//
//        dlqConsumer.use {
//            Assert.assertEventually(ThrowingSupplier {
//                val query = """
//                MATCH (c:Customer)
//                RETURN count(c) AS count
//            """.trimIndent()
//
//                val records = dlqConsumer.poll(5000)
//                val record = if (records.isEmpty) null else records.records(dlqTopic).iterator().next()
//                val headers = record?.headers()?.map { it.key() to String(it.value()) }?.toMap().orEmpty()
//                val value = if (record != null) JsonUtil.OBJECT_MAPPER.readValue(record.value()!!, Any::class.java) else emptyMap<String, Any>()
//                db.executeTransactionally(query, emptyMap()) {
//                    val result = it.columnAs<Long>("count")
//                    !records.isEmpty && headers.size == 8 && value == data && result.hasNext() && result.next() == 0L && !result.hasNext()
//                            && headers["__streams.errors.exception.class.name"] == "org.neo4j.graphdb.QueryExecutionException"
//                            && headers["__streams.errors.databaseName"] == "neo4j"
//                }
//            }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
//        }
//    }
//
//    @Test
//    fun `should send data to the DLQ because of JsonParseException`() {
//        val topic = UUID.randomUUID().toString()
//        val dlqTopic = UUID.randomUUID().toString()
//        db.setConfig("streams.sink.topic.cypher.$topic","MERGE (c:Customer {id: event.id})")
//        db.setConfig("streams.sink."+ErrorService.ErrorConfig.TOLERANCE, "all")
//        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_TOPIC, dlqTopic)
//        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADERS, "true")
//        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADER_PREFIX, "__streams.errors.")
//        db.start()
//
//        val data = """{id: 1, "name": "Andrea", "surname": "Santurbano"}"""
//
//        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(),
//                data.toByteArray())
//        kafkaProducer.send(producerRecord).get()
//        val dlqConsumer = KafkaTestUtils.createConsumer<ByteArray, ByteArray>(
//                bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
//                schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl(),
//                keyDeserializer = ByteArrayDeserializer::class.java.name,
//                valueDeserializer = ByteArrayDeserializer::class.java.name,
//                topics = *arrayOf(dlqTopic))
//        dlqConsumer.use {
//            Assert.assertEventually(ThrowingSupplier {
//                val query = """
//                MATCH (c:Customer)
//                RETURN count(c) AS count
//            """.trimIndent()
//                db.executeTransactionally(query, emptyMap()) { result ->
//                    val count = result.columnAs<Long>("count")
//                    val records = dlqConsumer.poll(5000)
//                    val record = if (records.isEmpty) null else records.records(dlqTopic).iterator().next()
//                    val headers = record?.headers()?.map { it.key() to String(it.value()) }?.toMap().orEmpty()
//                    val value = if (record != null) String(record.value()) else emptyMap<String, Any>()
//                    !records.isEmpty && headers.size == 8 && data == value && count.hasNext() && count.next() == 0L && !count.hasNext()
//                            && headers["__streams.errors.exception.class.name"] == "com.fasterxml.jackson.core.JsonParseException"
//                            && headers["__streams.errors.databaseName"] == "neo4j"
//                }
//            }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
//        }
//    }
//
//}