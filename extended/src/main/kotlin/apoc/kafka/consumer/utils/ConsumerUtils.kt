package apoc.kafka.consumer.utils

import org.neo4j.kernel.internal.GraphDatabaseAPI
import apoc.kafka.consumer.StreamsEventSinkAvailabilityListener
import apoc.kafka.utils.Neo4jUtils

object ConsumerUtils {

    fun isWriteableInstance(db: GraphDatabaseAPI): Boolean = Neo4jUtils
        .isWriteableInstance(db) { StreamsEventSinkAvailabilityListener.isAvailable(db) }

    fun <T> executeInWriteableInstance(db: GraphDatabaseAPI,
                                       action: () -> T?): T? = Neo4jUtils.executeInWriteableInstance(db,
        { StreamsEventSinkAvailabilityListener.isAvailable(db) }, action)

}