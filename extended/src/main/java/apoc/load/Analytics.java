package apoc.load;

import apoc.Extended;
import apoc.load.util.LoadJdbcConfig;
import apoc.result.RowResult;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.load.Jdbc.executeQuery;
import static apoc.load.Jdbc.executeUpdate;
import static apoc.load.util.JdbcUtil.getConnection;
import static apoc.load.util.JdbcUtil.getUrlOrKey;

@Extended
public class Analytics {

    enum Provider {
        POSTGRES,
        DUCKDB,
        MYSQL
    }

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure("apoc.load.jdbc.analytics")
    @Description("apoc.load.jdbc.analytics(<cypherQuery>, <jdbcUrl>, <sqlQueryOverTemporaryTable>, $config) - to create a temporary table starting from a Cypher query and delegate complex analytics to the database defined JDBC URL ")
    public Stream<RowResult> aggregate(
            @Name("neo4jQuery") String neo4jQuery,
            @Name("jdbc") String urlOrKey,
            @Name("sqlQuery") String sqlQuery,
            @Name(value = "params", defaultValue = "[]") List<Object> params,
            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) throws Exception {
        AtomicReference<String> createTable = new AtomicReference<>("");
        final Provider provider = Provider.valueOf((String) config.getOrDefault("provider", Provider.DUCKDB.name()));

        createTable.set("CREATE TEMPORARY TABLE temp_table ");

        AtomicReference<String> columns = new AtomicReference<>();
        Map<String, String> sqlTypes = new LinkedHashMap<>();
        AtomicReference<String> queryInsert = new AtomicReference<>("INSERT INTO temp_table VALUES ");
                db.executeTransactionally(neo4jQuery,
                Map.of(),
                r -> {
                    List<String> sqlValues = new ArrayList<>();
                    r.forEachRemaining(map -> {

                        map.entrySet().stream()
                                .sorted(Map.Entry.comparingByKey())
                                .forEachOrdered(x -> sqlTypes.put(x.getKey(), mapSqlType(provider, x.getValue())));

                        final Collection<Object> values = map.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList();
                        final String row = values.stream().map(x -> {
                            final String stringValue = x.toString();
                            if (x instanceof Number) return stringValue;
                            return String.format("'%s'", stringValue.replace("'", "''"));
                        }).collect(Collectors.joining(","));
                        sqlValues.add("(" + row + ")");
                    });
                    queryInsert.set(queryInsert.get() + StringUtils.join(sqlValues, ","));
                    columns.set(r.columns().stream().sorted().collect(Collectors.joining(",")));
                    return null;
                });

        createTable.set(createTable.get() + mapToString(sqlTypes));

        String url = getUrlOrKey(urlOrKey);
        LoadJdbcConfig jdbcConfig = new LoadJdbcConfig(config);
        Connection connection = (Connection) getConnection(url, jdbcConfig, Connection.class);

        // Create temporary table
        executeUpdate(urlOrKey, createTable.get(), config, log, connection, params.toArray(new Object[params.size()]));

        // Insert data
        executeUpdate(urlOrKey, queryInsert.get(), config, log, connection, params.toArray(new Object[params.size()]));

        try {
            return executeQuery(urlOrKey, sqlQuery, config, log, connection, params.toArray(new Object[params.size()]));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Make sure the SQL is consistent with Cypher query which has columns: %s", columns.get()));
        }
    }

    private String mapSqlType(Provider provider, Object value) {
        return switch (provider) {
            case MYSQL, POSTGRES -> {
                if (value instanceof Number) yield "INTEGER";
                else yield "VARCHAR(1000)";
            }
            default -> {
                if (value instanceof Number) yield "INTEGER";
                else yield "VARCHAR";
            }
        };
    }

    public String mapToString(Map<String, ?> map) {
        String mapAsString = map.keySet().stream()
                .map(key -> key + " " + map.get(key))
                .collect(Collectors.joining(", ", "(", ")"));
        return mapAsString;
    }


    /* TODO scrivere questa cosa sulla PR:
        meglio non aggregation, così è più personalizzabile, posso scegliere quali risultati ottenere e come ottenerli 
        altrimenti per fare qualcosa come sotto, con movies_count dovrei mettere un parametri aggKeys e fare cose strane
        
            MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
            RETURN 
                p.name AS actor, 
                m.genre AS genre, 
                r.roles AS roles, 
                COUNT(m) AS movies_count
     */


    
}
