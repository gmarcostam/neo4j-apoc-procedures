package apoc.kafka.consumer.utils

import org.neo4j.kernel.internal.GraphDatabaseAPI
import apoc.kafka.consumer.StreamsEventSinkAvailabilityListener
import apoc.kafka.utils.KafkaUtil

object ConsumerUtils {

    fun isWriteableInstance(db: GraphDatabaseAPI): Boolean = KafkaUtil
        .isWriteableInstance(db) { StreamsEventSinkAvailabilityListener.isAvailable(db) }

    fun <T> executeInWriteableInstance(db: GraphDatabaseAPI,
                                       action: () -> T?): T? = KafkaUtil.executeInWriteableInstance(db,
        { StreamsEventSinkAvailabilityListener.isAvailable(db) }, action)

}