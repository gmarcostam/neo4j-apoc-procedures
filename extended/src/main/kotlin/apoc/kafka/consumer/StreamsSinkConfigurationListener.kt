package apoc.kafka.consumer

import apoc.kafka.config.StreamsConfig
import apoc.kafka.consumer.kafka.KafkaEventSink
import apoc.kafka.consumer.kafka.KafkaSinkConfiguration
import apoc.kafka.consumer.procedures.StreamsSinkProcedures
import apoc.kafka.consumer.utils.ConsumerUtils
import apoc.kafka.extensions.isDefaultDb
import apoc.kafka.utils.KafkaUtil
import apoc.kafka.utils.KafkaUtil.getProducerProperties
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.commons.configuration2.ImmutableConfiguration
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.plugin.configuration.ConfigurationLifecycleUtils
import org.neo4j.plugin.configuration.EventType
import org.neo4j.plugin.configuration.listners.ConfigurationLifecycleListener

class StreamsSinkConfigurationListener(private val db: GraphDatabaseAPI,
                                       private val log: Log) : ConfigurationLifecycleListener {

    private val mutex = Mutex()

    var eventSink: KafkaEventSink? = null

    private val streamsTopicService = StreamsTopicService()

    private var lastConfig: KafkaSinkConfiguration? = null

    private val producerConfig = getProducerProperties()

    private fun KafkaSinkConfiguration.excludeSourceProps() = this.asProperties()
        ?.filterNot { producerConfig.contains(it.key) || it.key.toString().startsWith("apoc.kafka.source") }

    // visible for testing
    fun isConfigurationChanged(configMap: Map<String, String>) = when (configMap
        .getOrDefault("apoc.kafka.sink", "apoc.kafka.consumer.kafka.KafkaEventSink")) {
        "apoc.kafka.consumer.kafka.KafkaEventSink" ->  {
            // we validate all properties except for the ones related to the Producer
            // we use this strategy because there are some properties related to the Confluent Platform
            // that we're not able to track from the Apache Packages
            // i.e. the Schema Registry
            val kafkaConfig = KafkaSinkConfiguration.create(configMap, dbName = db.databaseName(), isDefaultDb = db.isDefaultDb())
            val config = kafkaConfig.excludeSourceProps()
            val lastConfig = this.lastConfig?.excludeSourceProps()
            val streamsConfig = kafkaConfig.sinkConfiguration
            config != lastConfig || streamsConfig != this.lastConfig?.sinkConfiguration
        }
        else -> true
    }

    override fun onShutdown() {
        runBlocking {
            mutex.withLock {
                shutdown()
            }
        }
    }

    fun shutdown() {
        val isShuttingDown = eventSink != null
        if (isShuttingDown) {
            log.info("[Sink] Shutting down the Streams Sink Module")
        }
        eventSink?.stop()
        eventSink = null
        StreamsSinkProcedures.unregisterStreamsEventSink(db)
        if (isShuttingDown) {
            log.info("[Sink] Shutdown of the Streams Sink Module completed")
        }
    }

    override fun onConfigurationChange(evt: EventType, config: ImmutableConfiguration) {
        if (config.isEmpty) {
            if (log.isDebugEnabled) {
                log.debug("[Sink] Configuration is empty")
            }
            return
        }
        runBlocking {
            mutex.withLock {
                log.info("[Sink] An event change is detected ${evt.name}")
                val configMap = ConfigurationLifecycleUtils.toMap(config)
                    .mapValues { it.value.toString() }
                if (!isConfigurationChanged(configMap)) {
                    log.info("[Sink] The configuration is not changed so the module will not restarted")
                    return@runBlocking
                }
                shutdown()
                if (log.isDebugEnabled) {
                    log.debug("[Sink] The new configuration is: $configMap")
                }
                start(configMap)
            }
        }
    }

    fun start(configMap: Map<String, String>) {
        lastConfig = KafkaSinkConfiguration.create(StreamsConfig.getConfiguration(), db.databaseName(), db.isDefaultDb())
        val streamsSinkConfiguration = lastConfig!!.sinkConfiguration
        streamsTopicService.clearAll()
        streamsTopicService.setAll(streamsSinkConfiguration.topics)

        val neo4jStrategyStorage = Neo4jStreamsStrategyStorage(streamsTopicService, configMap, db)
        val streamsQueryExecution = StreamsEventSinkQueryExecution(db,
            log, neo4jStrategyStorage)

        eventSink = StreamsEventSinkFactory
            .getStreamsEventSink(configMap,
                streamsQueryExecution,
                streamsTopicService,
                log,
                db)
        try {
            if (streamsSinkConfiguration.enabled) {
                log.info("[Sink] The Streams Sink module is starting")
                if (KafkaUtil.isCluster(db)) {
                    initSinkModule(streamsSinkConfiguration)
                } else {
                    runInASingleInstance(streamsSinkConfiguration)
                }
            }
        } catch (e: Exception) {
            log.warn("Cannot start the Streams Sink module because the following exception", e)
        }

        log.info("[Sink] Registering the Streams Sink procedures")
        StreamsSinkProcedures.registerStreamsEventSink(db, eventSink!!)
    }

    private fun initSink() {
        eventSink?.start()
        eventSink?.printInvalidTopics()
    }

    private fun runInASingleInstance(streamsSinkConfiguration: StreamsSinkConfiguration) {
        // check if is writeable instance
        ConsumerUtils.executeInWriteableInstance(db) {
            if (streamsSinkConfiguration.clusterOnly) {
                log.info("""
                        |Cannot init the Streams Sink module as is forced to work only in a cluster env, 
                        |please check the value of `${StreamsConfig.CLUSTER_ONLY}`
                    """.trimMargin())
            } else {
                initSinkModule(streamsSinkConfiguration)
            }
        }
    }

    private fun initSinkModule(streamsSinkConfiguration: StreamsSinkConfiguration) {
            initSink()
    }
}