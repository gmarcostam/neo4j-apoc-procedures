package apoc.kafka.producer.integrations

import apoc.kafka.PublishProcedures
import apoc.kafka.events.OperationType
import apoc.kafka.events.StreamsTransactionEvent
import apoc.kafka.support.KafkaTestUtils
import apoc.util.DbmsTestUtil
import apoc.util.TestUtil
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.rules.TemporaryFolder
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestDatabaseManagementServiceBuilder

open class KafkaEventRouterBaseTSE { // TSE (Test Suit Element)

    companion object {

        private var startedFromSuite = true

        private fun getDbServices() : GraphDatabaseService {
            val db = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
            TestUtil.registerProcedure(db, PublishProcedures::class.java);
            return db
        }



//        @JvmField
//        @ClassRule
//        var STORE_DIR = TemporaryFolder()

        lateinit var db: GraphDatabaseService
        lateinit var dbms: DatabaseManagementService
        
        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            if (!KafkaEventRouterSuiteIT.isRunning) {
                startedFromSuite = false
                KafkaEventRouterSuiteIT.setUpContainer()
            }

//            dbms = DbmsTestUtil.startDbWithApocConfigs(
//                temporaryFolder,
//                Map.of<String, Any>("kafka.bootstrap.servers", KafkaEventRouterSuiteIT.kafka.bootstrapServers,
//                    "bootstrap.servers", KafkaEventRouterSuiteIT.kafka.bootstrapServers)
//            )
//            getDbServices()

        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            if (!startedFromSuite) {
                KafkaEventRouterSuiteIT.tearDownContainer()
            }

//            dbms.shutdown()
        }

        // common methods
        fun isValidRelationship(event: StreamsTransactionEvent, type: OperationType) = when (type) {
            OperationType.created -> event.payload.before == null
                    && event.payload.after?.let { it.properties?.let { it.isNullOrEmpty() } } ?: false
                    && event.schema.properties == emptyMap<String, String>()
            OperationType.updated -> event.payload.before?.let { it.properties?.let { it.isNullOrEmpty() } } ?: false
                    && event.payload.after?.let { it.properties == mapOf("type" to "update") } ?: false
                    && event.schema.properties == mapOf("type" to "String")
            OperationType.deleted -> event.payload.before?.let { it.properties == mapOf("type" to "update") } ?: false
                    && event.payload.after == null
                    && event.schema.properties == mapOf("type" to "String")
            else -> throw IllegalArgumentException("Unsupported OperationType")
        }
    }

    
//    @JvmField
//    @Rule
//    val db: DbmsRule = ImpermanentDbmsRule()

    lateinit var kafkaConsumer: KafkaConsumer<String, ByteArray>

    @JvmField
    @Rule
    var temporaryFolder = TemporaryFolder()
    
    @Before
    @BeforeEach
    fun setUp() {
//        getDbServices()

//        ApocConfig.apocConfig().setProperty("kafka.bootstrap.servers", KafkaEventRouterSuiteIT.kafka.bootstrapServers)
        kafkaConsumer = KafkaTestUtils.createConsumer(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
    }


    @After
    @AfterEach
    fun tearDown() {
        dbms.shutdown()
        
        dbms = TestDatabaseManagementServiceBuilder(temporaryFolder.root.toPath()).build()
        getDbServices()
        
        kafkaConsumer.close()
//        db.executeTransactionally("MATCH (n) DETACH DELETE n")
    }

    fun createDbWithKafkaConfigs(vararg pairs: Pair<String, Any>) : GraphDatabaseService {
        val mutableMapOf = mutableMapOf<String, Any>(
            "kafka.bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers,
            "bootstrap.servers" to  KafkaEventRouterSuiteIT.kafka.bootstrapServers
//            "kafka.bootstrap.servers" to KafkaEventSinkSuiteIT.kafka.bootstrapServers,
//            "bootstrap.servers" to KafkaEventSinkSuiteIT.kafka.bootstrapServers,
//            "streams.sink.enabled" to "true"
//            ,
//            "kafka.key.deserializer" to StringDeserializer::class.java.name
//            ,
//            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
//            "key.deserializer" to ByteArrayDeserializer::class.java.name

            // todo - "kafka.key.deserializer" deve serializzare una stringa, oltre che un object!!

            //                "streams.sink.topic.cypher.avro", "CREATE (p:Place{name: event.name, coordinates: event.coordinates, citizens: event.citizens})",
            //                "kafka.key.deserializer", KafkaAvroDeserializer::class.java.name,
            //                "kafka.value.deserializer", KafkaAvroDeserializer::class.java.name,
            //                "kafka.schema.registry.url", KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl()
        )
        
        mutableMapOf.putAll(mapOf(*pairs))


        dbms = DbmsTestUtil.startDbWithApocConfigs(
            temporaryFolder,
            mutableMapOf
        )

//        var db: GraphDatabaseService
        return getDbServices()
//        return db
    }
}