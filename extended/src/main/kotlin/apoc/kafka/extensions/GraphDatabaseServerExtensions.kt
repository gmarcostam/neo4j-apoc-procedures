package apoc.kafka.extensions

import org.neo4j.common.DependencyResolver
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.event.TransactionEventListener
import org.neo4j.kernel.internal.GraphDatabaseAPI
import apoc.kafka.utils.StreamsUtils

fun GraphDatabaseService.execute(cypher: String) = this.execute(cypher, emptyMap())
fun GraphDatabaseService.execute(cypher: String, params: Map<String, Any>) = this.executeTransactionally(cypher, params)

fun <T> GraphDatabaseService.execute(cypher: String, lambda: ((Result) -> T)) = this.execute(cypher, emptyMap(), lambda)
fun <T> GraphDatabaseService.execute(cypher: String,
                                     params: Map<String, Any>,
                                     lambda: ((Result) -> T)) = this.executeTransactionally(cypher, params, lambda)

fun GraphDatabaseService.isSystemDb() = this.databaseName() == StreamsUtils.SYSTEM_DATABASE_NAME

fun GraphDatabaseService.databaseManagementService() = (this as GraphDatabaseAPI).dependencyResolver
    .resolveDependency(DatabaseManagementService::class.java, DependencyResolver.SelectionStrategy.SINGLE)

fun GraphDatabaseService.isDefaultDb() = databaseManagementService().getDefaultDbName() == databaseName()

fun GraphDatabaseService.registerTransactionEventListener(txHandler: TransactionEventListener<*>) {
    databaseManagementService().registerTransactionEventListener(this.databaseName(), txHandler)
}

fun GraphDatabaseService.unregisterTransactionEventListener(txHandler: TransactionEventListener<*>) {
    databaseManagementService().unregisterTransactionEventListener(this.databaseName(), txHandler)
}