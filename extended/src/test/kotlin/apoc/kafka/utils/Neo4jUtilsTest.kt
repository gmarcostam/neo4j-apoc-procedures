package apoc.kafka.utils

import org.junit.ClassRule
import org.junit.Test
import org.neo4j.test.rule.ImpermanentDbmsRule
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Neo4jUtilsTest {

    companion object {
        @ClassRule @JvmField
        val db = ImpermanentDbmsRule()
    }

    @Test
    fun shouldCheckIfIsWriteableInstance() {
        val isWriteableInstance = Neo4jUtils.isWriteableInstance(db)
        assertTrue { isWriteableInstance }
    }
//
//    @Test
//    fun `should not have APOC`() {
//        assertFalse { Neo4jUtils.hasApoc(db) }
//    }

}