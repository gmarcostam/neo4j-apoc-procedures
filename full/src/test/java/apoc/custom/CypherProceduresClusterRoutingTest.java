package apoc.custom;

import static apoc.custom.CypherProceduresHandler.CUSTOM_PROCEDURES_REFRESH;
import static apoc.util.TestContainerUtil.testCallEmpty;
import static apoc.util.TestContainerUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.internal.helpers.collection.Iterators;

public class CypherProceduresClusterRoutingTest {
    private static final int NUM_CORES = 3;
    private static TestcontainersCausalCluster cluster;
    private static Session clusterSession;
    private static List<Neo4jContainerExtension> members;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil.createEnterpriseCluster(
                List.of(TestContainerUtil.ApocPackage.FULL),
                NUM_CORES,
                0,
                Collections.emptyMap(),
                Map.of(
                        "NEO4J_initial_server_mode__constraint",
                        "PRIMARY",
                        "NEO4J_dbms_routing_enabled",
                        "true",
                        CUSTOM_PROCEDURES_REFRESH,
                        "1000"));

        clusterSession = cluster.getSession();
        members = cluster.getClusterMembers();

        assertEquals(NUM_CORES, members.size());
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }

    @Test
    public void testCustomInstallAndDropAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.custom.installProcedure($name, 'RETURN 42 AS answer')";
        final String queryFun = "CALL apoc.custom.installFunction($name, 'RETURN 42 AS answer')";
        customInSysLeaderMemberCommon((session, name) -> {
            // create customs
            Map<String, Object> paramsProc = Map.of("name", name + "() :: (answer::ANY)");
            testCallEmpty(session, query, paramsProc);

            Map<String, Object> paramsFun = Map.of("name", name + "() :: INT");
            testCallEmpty(session, queryFun, paramsFun);

            // drop customs
            String queryDropProc = "CALL apoc.custom.dropProcedure($name)";
            testCallEmpty(session, queryDropProc, paramsProc);

            String queryDropFun = "CALL apoc.custom.dropFunction($name)";
            testCallEmpty(session, queryDropFun, paramsProc);
        });
    }

    @Test
    public void testCustomShowAllowedInAllSysLeaderMembers() {
        final String query = "CALL apoc.custom.show";
        final BiConsumer<Session, String> testUuidShow =
                (session, name) -> testResult(session, query, Iterators::count);
        customInSysLeaderMemberCommon(testUuidShow, true);
    }

    private static void customInSysLeaderMemberCommon(BiConsumer<Session, String> testUuid) {
        customInSysLeaderMemberCommon(testUuid, false);
    }

    private static void customInSysLeaderMemberCommon(BiConsumer<Session, String> testUuid, boolean readOnlyOperation) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(NUM_CORES, members.size());
        for (Neo4jContainerExtension container : members) {
            final String name = getUniqueName(container);
            // we skip READ_REPLICA members with readOnlyOperation=false
            final Driver driver = readOnlyOperation ? container.getDriver() : getDriverIfNotReplica(container);
            if (driver == null) {
                continue;
            }
            Session session = driver.session(SessionConfig.forDatabase(SYSTEM_DATABASE_NAME));
            boolean isWriter = dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(container));
            if (readOnlyOperation || isWriter) {
                testUuid.accept(session, name);
            } else {
                try {
                    testUuid.accept(session, name);
                    fail("Should fail because of non leader custom procedure/function addition");
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    assertTrue(
                            "The actual message is: " + errorMsg,
                            errorMsg.contains(apoc.util.SystemDbUtil.PROCEDURE_NOT_ROUTED_ERROR));
                }
            }
        }
    }

    public static String getBoltAddress(Neo4jContainerExtension instance) {
        return instance.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
    }

    public static boolean dbIsWriter(String dbName, Session session, String boltAddress) {
        return session.run(
                        "SHOW DATABASE $dbName WHERE address = $boltAddress",
                        Map.of("dbName", dbName, "boltAddress", boltAddress))
                .single()
                .get("writer")
                .asBoolean();
    }

    private static Driver getDriverIfNotReplica(Neo4jContainerExtension container) {
        final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
        final Driver driver = container.getDriver();
        if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || driver == null) {
            return null;
        }
        return driver;
    }

    private static boolean sysIsLeader(Session session) {
        final String systemRole =
                TestContainerUtil.singleResultFirstColumn(session, "CALL dbms.cluster.role('system')");
        return "LEADER".equals(systemRole);
    }

    private static String getUniqueName(Neo4jContainerExtension container) {
        return "proc" + container.getMappedPort(7687).toString();
    }
}
