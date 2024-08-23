//package apoc.kafka.serialization
//
//import apoc.kafka.events.*
//import apoc.kafka.utils.JSONUtils
//import apoc.util.JsonUtil
//import org.junit.Test
//import org.neo4j.driver.Values
//import org.neo4j.values.storable.CoordinateReferenceSystem.*
//import org.neo4j.values.storable.DateTimeValue.datetime
//import org.neo4j.values.storable.DateValue.date
//import org.neo4j.values.storable.TimeValue.time
//import org.neo4j.values.storable.Values.pointValue
//import java.time.ZoneOffset.UTC
//import kotlin.test.assertEquals
//import kotlin.test.assertFails
//
//class JSONUtilsTest {
//
//    @Test
//    fun `should serialize Geometry and Temporal Data Types`() {
//        // Given
//        val expected = "{\"point2dCartesian\":{\"crs\":\"cartesian\",\"x\":1.0,\"y\":2.0,\"z\":null}," +
//                "\"point3dCartesian\":{\"crs\":\"cartesian-3d\",\"x\":1.0,\"y\":2.0,\"z\":3.0}," +
//                "\"point2dWgs84\":{\"crs\":\"wgs-84\",\"latitude\":1.0,\"longitude\":2.0,\"height\":null}," +
//                "\"point3dWgs84\":{\"crs\":\"wgs-84-3d\",\"latitude\":1.0,\"longitude\":2.0,\"height\":3.0}," +
//                "\"time\":\"14:00:00Z\",\"dateTime\":\"2017-12-17T17:14:35.123456789Z\"}"
//        val map = linkedMapOf<String, Any>("point2dCartesian" to pointValue(CARTESIAN, 1.0, 2.0),
//                "point3dCartesian" to pointValue(CARTESIAN_3D, 1.0, 2.0, 3.0),
//                "point2dWgs84" to pointValue(WGS_84, 1.0, 2.0),
//                "point3dWgs84" to pointValue(WGS_84_3D, 1.0, 2.0, 3.0),
//                "time" to time(14, 0, 0, 0, UTC),
//                "dateTime" to datetime(date(2017, 12, 17), time(17, 14, 35, 123456789, UTC)))
//
//        // When
//        val jsonString = JsonUtil.writeValueAsString(map)
//
//        // Then
//        assertEquals(expected, jsonString)
//    }
//
//    @Test
//    fun `should serialize driver Point Data Types`() {
//        // Given
//        val expected = "{\"point2dCartesian\":{\"crs\":\"cartesian\",\"x\":1.0,\"y\":2.0,\"z\":null}," +
//                "\"point3dCartesian\":{\"crs\":\"cartesian-3d\",\"x\":1.0,\"y\":2.0,\"z\":3.0}," +
//                "\"point2dWgs84\":{\"crs\":\"wgs-84\",\"latitude\":1.0,\"longitude\":2.0,\"height\":null}," +
//                "\"point3dWgs84\":{\"crs\":\"wgs-84-3d\",\"latitude\":1.0,\"longitude\":2.0,\"height\":3.0}," +
//                "\"time\":\"14:00:00Z\",\"dateTime\":\"2017-12-17T17:14:35.123456789Z\"}"
//        val map = linkedMapOf<String, Any>("point2dCartesian" to pointValue(CARTESIAN, 1.0, 2.0),
//                "point3dCartesian" to Values.point(CARTESIAN_3D.code, 1.0, 2.0, 3.0),
//                "point2dWgs84" to Values.point(WGS_84.code, 1.0, 2.0),
//                "point3dWgs84" to Values.point(WGS_84_3D.code, 1.0, 2.0, 3.0),
//                "time" to time(14, 0, 0, 0, UTC),
//                "dateTime" to datetime(date(2017, 12, 17), time(17, 14, 35, 123456789, UTC)))
//
//        // When
//        val jsonString = JsonUtil.writeValueAsString(map)
//
//        // Then
//        assertEquals(expected, jsonString)
//    }
//
//    @Test
//    fun `should convert data to StreamsTransactionEvent`() {
//        // given
//        val timestamp = System.currentTimeMillis()
//        val cdcData = StreamsTransactionEvent(
//                meta = Meta(timestamp = timestamp,
//                        username = "user",
//                        txId = 1,
//                        txEventId = 0,
//                        txEventsCount = 1,
//                        operation = OperationType.created
//                ),
//                payload = NodePayload(id = "0",
//                        before = null,
//                        after = NodeChange(properties = mapOf("prop1" to "foo", "bar" to 1), labels = listOf("LabelCDC")),
//                        type = EntityType.node
//                ),
//                schema = Schema()
//        )
//        val cdcMap = mapOf<String, Any>(
//                "meta" to mapOf("timestamp" to timestamp,
//                        "username" to "user",
//                        "txId" to 1,
//                        "txEventId" to 0,
//                        "txEventsCount" to 1,
//                        "operation" to OperationType.created),
//                "payload" to mapOf("id" to "0",
//                        "before" to null,
//                        "after" to NodeChange(properties = mapOf("prop1" to "foo", "bar" to 1), labels = listOf("LabelCDC")),
//                        "type" to EntityType.node),
//                "schema" to emptyMap<String, Any>()
//        )
//        val cdcString = """{
//            |"meta":{"timestamp":$timestamp,"username":"user","txId":1,"txEventId":0,"txEventsCount":1,"operation":"created"},
//            |"payload":{"id":"0","before":null,"after":{"properties":{"prop1":"foo","bar":1},"labels":["LabelCDC"]},"type":"node"},
//            |"schema":{}
//            |}""".trimMargin()
//
//        val fromMap = JSONUtils.asStreamsTransactionEvent(cdcMap)
//        val fromString = JSONUtils.asStreamsTransactionEvent(cdcString)
//        assertEquals(cdcData, fromMap)
//        assertEquals(cdcData, fromString)
//    }
//
//    @Test
//    fun `should deserialize plain values`() {
//        // TODO: implement Kotlin JsonUtil
//        assertEquals("a", JsonUtil.OBJECT_MAPPER.readValue("a", Any::class.java))
//        assertEquals(42, JsonUtil.OBJECT_MAPPER.readValue("42", Any::class.java))
//        assertEquals(true,  JSONUtils.readValue("true"))
//        assertFails { JSONUtils.readValue("a") }
//        // assertEquals(null, JSONUtils.readValue("null"))
//    }
//}