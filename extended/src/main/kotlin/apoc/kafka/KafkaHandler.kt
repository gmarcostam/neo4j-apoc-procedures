package apoc.kafka

import apoc.TTLConfig
import apoc.kafka.config.StreamsConfig
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
        initListeners(db, log)
    }

    override fun stop() {
        println("stop db..........")
        // TODO - mettere i shutdown di StreamsRouterConfigurationListener e StreamsSinkConfigurationListener
        
    }

    fun initListeners(db: GraphDatabaseAPI?, log: Log?) {
        // todo - check if there is a better way, 
        //  maybe with if( ApocConfig.apocConfig().getBoolean("apoc.kafka.enabled") )
        StreamsRouterConfigurationListener(db!!, log!!
        ).start(StreamsConfig.getConfiguration())

        StreamsSinkConfigurationListener(db!!, log!!
        ).start(StreamsConfig.getConfiguration())
    }
}