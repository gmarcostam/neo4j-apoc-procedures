package apoc.kafka.utils

import apoc.kafka.events.Constraint
import apoc.kafka.events.RelKeyStrategy
import apoc.kafka.events.StreamsConstraintType
import apoc.kafka.events.StreamsTransactionEvent
import apoc.kafka.service.StreamsSinkEntity

object SchemaUtils {
    fun getNodeKeys(labels: List<String>, propertyKeys: Set<String>, constraints: List<Constraint>, keyStrategy: RelKeyStrategy = RelKeyStrategy.DEFAULT): Set<String> =
            constraints
                .filter { constraint ->
                    constraint.type == StreamsConstraintType.UNIQUE
                            && propertyKeys.containsAll(constraint.properties)
                            && labels.contains(constraint.label)
                }
                .let {
                    when(keyStrategy) {
                        RelKeyStrategy.DEFAULT -> {
                            // we order first by properties.size, then by label name and finally by properties name alphabetically
                            // with properties.sorted() we ensure that ("foo", "bar") and ("bar", "foo") are no different
                            // with toString() we force it.properties to have the natural sort order, that is alphabetically
                            it.minWithOrNull((compareBy({ it.properties.size }, { it.label }, { it.properties.sorted().toString() })))
                                    ?.properties
                                    .orEmpty()
                        }
                        // with 'ALL' strategy we get a set with all properties
                        RelKeyStrategy.ALL -> it.flatMap { it.properties }.toSet()
                    }
                }


    fun toStreamsTransactionEvent(streamsSinkEntity: StreamsSinkEntity,
                                  evaluation: (StreamsTransactionEvent) -> Boolean)
            : StreamsTransactionEvent? = if (streamsSinkEntity.value != null) {
        val data = JSONUtils.asStreamsTransactionEvent(streamsSinkEntity.value)
        if (evaluation(data)) data else null
    } else {
        null
    }

}