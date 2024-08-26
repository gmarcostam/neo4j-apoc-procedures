package apoc.kafka

import apoc.ApocConfig
import apoc.kafka.config.StreamsConfig
import apoc.kafka.consumer.StreamsEventSinkAvailabilityListener
import apoc.kafka.consumer.StreamsSinkConfigurationListener
import apoc.kafka.producer.StreamsRouterConfigurationListener
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.Log

class KafkaHandler(): LifecycleAdapter() {

    private lateinit var db: GraphDatabaseAPI
    private lateinit var log: Log

    constructor(db: GraphDatabaseAPI, log: Log) : this() {
        this.db = db
        this.log = log
    }

    override fun start() {
        if(ApocConfig.apocConfig().getBoolean("apoc.kafka.enabled")) {
            println("start db......")

            StreamsEventSinkAvailabilityListener.setAvailable(db , true);

            try {
                StreamsRouterConfigurationListener(db, log)
                    .start(StreamsConfig.getConfiguration())
            } catch (e: Exception) {
                log.error("Exception in StreamsRouterConfigurationListener {}", e.message)
            }

            try {
                StreamsSinkConfigurationListener(db, log)
                    .start(StreamsConfig.getConfiguration())
            } catch (e: Exception) {
                log.error("Exception in StreamsSinkConfigurationListener {}", e.message)
            }
        }
    }

    override fun stop() {
        if(ApocConfig.apocConfig().getBoolean("apoc.kafka.enabled")) {
            println("stop db..........")

            StreamsEventSinkAvailabilityListener.setAvailable(db, false)
            StreamsRouterConfigurationListener(db, log).shutdown()
            StreamsSinkConfigurationListener(db, log).shutdown()
        }
    }
}