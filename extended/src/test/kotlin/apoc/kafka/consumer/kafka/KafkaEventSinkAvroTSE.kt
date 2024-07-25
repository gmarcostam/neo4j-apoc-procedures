package apoc.kafka.consumer.kafka

import apoc.ApocConfig
import apoc.kafka.producer.integrations.KafkaEventSinkSuiteIT
import apoc.kafka.support.Assert

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.Node
import java.util.*
import java.util.concurrent.TimeUnit


class KafkaEventSinkAvroTSE : KafkaEventSinkBaseTSE() {

    @Test
    fun `should insert AVRO data`() {
        //given
        val topic = "avro"
        ApocConfig.apocConfig().setProperty("streams.sink.topic.cypher.$topic", "CREATE (p:Place{name: event.name, coordinates: event.coordinates, citizens: event.citizens})")
        ApocConfig.apocConfig().setProperty("kafka.key.deserializer", KafkaAvroDeserializer::class.java.name)
        ApocConfig.apocConfig().setProperty("kafka.value.deserializer", KafkaAvroDeserializer::class.java.name)
        ApocConfig.apocConfig().setProperty("kafka.schema.registry.url", KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl())

        val PLACE_SCHEMA = SchemaBuilder.builder("com.namespace")
                .record("Place").fields()
                .name("name").type().stringType().noDefault()
                .name("coordinates").type().array().items().doubleType().noDefault()
                .name("citizens").type().longType().noDefault()
                .endRecord()
        val coordinates = listOf(42.30000, -11.22222)
        val citizens = 1_000_000L
        val struct = GenericRecordBuilder(PLACE_SCHEMA)
                .set("name", "Foo")
                .set("coordinates", coordinates)
                .set("citizens", citizens)
                .build()

        // when
        kafkaAvroProducer.send(ProducerRecord<GenericRecord, GenericRecord>(topic, null, struct)).get()

        // then
        val props = mapOf("name" to "Foo", "coordinates" to coordinates.toDoubleArray(), "citizens" to citizens)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH (p:Place)
                |RETURN p""".trimMargin()
            val result = db.beginTx().use {
                val result = it.execute(query).columnAs<Node>("p")
                if (result.hasNext()) {
                    result.next().allProperties
                } else {
                    emptyMap()
                }
            }
            result.isNotEmpty() &&
                    props["name"] as String == result["name"] as String &&
                    props["coordinates"] as DoubleArray contentEquals result["coordinates"] as DoubleArray &&
                    props["citizens"] as Long == result["citizens"] as Long
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `the node pattern strategy must work also with AVRO data`() {
        //given
        val topic = UUID.randomUUID().toString()
        ApocConfig.apocConfig().setProperty("kafka.key.deserializer", KafkaAvroDeserializer::class.java.name)
        ApocConfig.apocConfig().setProperty("kafka.value.deserializer", KafkaAvroDeserializer::class.java.name)
        ApocConfig.apocConfig().setProperty("kafka.schema.registry.url", KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl())
        ApocConfig.apocConfig().setProperty("streams.sink.topic.pattern.node.$topic", "(:Place{!name})")
        // db.start()

        val PLACE_SCHEMA = SchemaBuilder.builder("com.namespace")
                .record("Place").fields()
                .name("name").type().stringType().noDefault()
                .name("coordinates").type().array().items().doubleType().noDefault()
                .name("citizens").type().longType().noDefault()
                .endRecord()
        val coordinates = listOf(42.30000, -11.22222)
        val citizens = 1_000_000L
        val struct = GenericRecordBuilder(PLACE_SCHEMA)
                .set("name", "Foo")
                .set("coordinates", coordinates)
                .set("citizens", citizens)
                .build()

        // when
        kafkaAvroProducer.send(ProducerRecord<GenericRecord, GenericRecord>(topic, null, struct)).get()

        // then
        val props = mapOf("name" to "Foo", "coordinates" to coordinates.toDoubleArray(), "citizens" to citizens)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH (p:Place)
                |RETURN p""".trimMargin()
            val result = db.beginTx().use {
                val result = it.execute(query).columnAs<Node>("p")
                if (result.hasNext()) {
                    result.next().allProperties
                } else {
                    emptyMap()
                }
            }
            result.isNotEmpty() &&
                    props["name"] as String == result["name"] as String &&
                    props["coordinates"] as DoubleArray contentEquals result["coordinates"] as DoubleArray &&
                    props["citizens"] as Long == result["citizens"] as Long
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

}