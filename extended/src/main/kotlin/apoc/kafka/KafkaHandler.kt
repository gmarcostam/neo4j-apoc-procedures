package apoc.kafka

import apoc.TTLConfig
import apoc.kafka.config.StreamsConfig
import apoc.kafka.consumer.StreamsEventSinkAvailabilityListener
import apoc.kafka.consumer.StreamsSinkConfigurationListener
import apoc.kafka.producer.StreamsRouterConfigurationListener
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.Log
import org.neo4j.scheduler.Group
import org.neo4j.scheduler.JobHandle
import org.neo4j.scheduler.JobScheduler

class KafkaHandler(): LifecycleAdapter(), AvailabilityListener {
    override fun available() {
        TODO("Not yet implemented")
    }

    override fun unavailable() {
        TODO("Not yet implemented")
    }

    private val TTL_GROUP = Group.INDEX_UPDATING
    private var scheduler: JobScheduler? = null
    private var db: GraphDatabaseAPI? = null
    private val ttlIndexJobHandle: JobHandle<*>? = null
    private val ttlJobHandle: JobHandle<*>? = null
    private var ttlConfig: TTLConfig? = null
    private var log: Log? = null

    constructor(scheduler: JobScheduler?, db: GraphDatabaseAPI?, ttlConfig: TTLConfig?, log: Log?) : this() {
        this.scheduler = scheduler
        this.db = db
        this.ttlConfig = ttlConfig
        this.log = log
    }

    override fun start() {
        println("start db......")

        StreamsEventSinkAvailabilityListener.setAvailable(db!! , true);

        try {
            StreamsRouterConfigurationListener(db!!, log!!
            ).start(StreamsConfig.getConfiguration())
        } catch (e: Exception) {
            log?.error("Exception in StreamsRouterConfigurationListener {}", e.message)
        }

        try {
            StreamsSinkConfigurationListener(db!!, log!!
            ).start(StreamsConfig.getConfiguration())
        } catch (e: Exception) {
            log?.error("Exception in StreamsSinkConfigurationListener {}", e.message)
        }
    }

    override fun stop() {
        println("stop db..........")
        //         todo - mettere StreamsEventSinkAvailabilityListener.setAvailable(db, false);
        db?.let { StreamsEventSinkAvailabilityListener.setAvailable(it, false) }
        // TODO - mettere i shutdown di StreamsRouterConfigurationListener e StreamsSinkConfigurationListener
        StreamsRouterConfigurationListener(db!!, log!!).shutdown()
        StreamsSinkConfigurationListener(db!!, log!!).shutdown()
    }
}