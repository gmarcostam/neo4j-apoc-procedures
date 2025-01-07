package apoc.load;

import apoc.Extended;
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



// TODO - scrivere sulla pr che abbiamo testato anche le apoc.load.jdbc* con DuckDB e fixato eventuali errori

@Extended
public class Analytics {

    enum Provider {
        POSTGRES,
        DUCKDB
    }

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    // TODO - PRENDERE COME ESEMPI https://chatgpt.com/share/67530793-c0d0-800c-a4fc-9ae01e098de3
    // TODO - testare principalmente per DuckDB
    
    //         TODO poi provare a testare con altri db, scopiazzando i container da MySQLJdbcTest e  PostgresJdbcTest
    
    //          se necessario, mettere qualcosa tipo config.getOrDefault("database", "duckDB") 
    //              e fare degli if-else/switch/etc.. per differenziare le query sql
    
    @Procedure("apoc.load.jdbc.analytics")
    // TODO 
    @Description("TODO - DESCRIZIONE")
    public Stream<RowResult> aggregate(
            @Name("neo4jQuery") String neo4jQuery,
            @Name("jdbc") String urlOrKey,
            @Name("sqlQuery") String sqlQuery,
            @Name(value = "params", defaultValue = "[]") List<Object> params,
            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {

        // TODO - scrivere sulla PR: add handling: some SQL database like Microsoft SQL Server create temp table in a different way
        //      e.g. CREATE TABLE #table_name (column_name datatype);
        //      document it

        // TODO step 1: temp table creation partendo dalla neo4jQuery
        //  mettere al posto di query la creazione di una tabella temporanea
        //  facendo leva su tx.execute(..) o db.executeTransactionally(...) e recuperandosi i risultati
        AtomicReference<String> createTable = new AtomicReference<>("");
        final Provider provider = Provider.valueOf((String) config.getOrDefault("provider", Provider.DUCKDB.name()));

        switch (provider) {
            case POSTGRES -> {
                return null;
            }
            case DUCKDB -> createTable.set("""
                CREATE TABLE temp_table 
                """);
        }
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
                                .forEachOrdered(x -> sqlTypes.put(x.getKey(), mapSqlType(x.getValue())));

                        final Collection<Object> values = map.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList();
                        final String row = values.stream().map(x -> {
                            final String stringValue = x.toString();
                            if (x instanceof Number) return stringValue;
                            return String.format("'%s'", stringValue.replace("'", "''"));
                        }).collect(Collectors.joining(","));
                        sqlValues.add("(" + row + ")");
                    });
//                    createTable.set(createTable.get() + StringUtils.join(sqlValues, ","));
                    queryInsert.set(queryInsert.get() + StringUtils.join(sqlValues, ","));
                    columns.set(r.columns().stream().sorted().collect(Collectors.joining(",")));
//                    createTable.set(createTable.get() + " AS t("  + columns.get() + ");");
                    return null;
                });

        createTable.set(createTable.get() + mapToString(sqlTypes));



        /* ad esempio, se passo la query neo4j

            MATCH (n:Movie) RETURN n.actor as actor, n.genre as genre, COUNT(n) as movies_count

            CREATE TEMPORARY TABLE table_name (
                column_name datatype
            );

            SELECT 
                actor,
                genre,
                SUM(movies_count) AS movies_count
            FROM movies_data
            GROUP BY actor, genre
            ORDER BY movies_count DESC
        
        la tabella sarà qualcosa tipo:
            
            CREATE TEMPORARY TABLE movies_data AS 
            SELECT * FROM 
            (VALUES
                ('Keanu Reeves', 'Sci-Fi', 3),
                ('Carrie-Anne Moss', 'Sci-Fi', 2),
                ('Laurence Fishburne', 'Sci-Fi', 3),
                ('Keanu Reeves', 'Action', 4),
                ('Will Smith', 'Action', 5)
            ) AS t(actor, genre, movies_count);
            
         */
        final Stream<RowResult> rowResultStream = executeUpdate(urlOrKey,
                createTable.get(), config
                , log, params.toArray(new Object[params.size()]));
        final RowResult rowResult = rowResultStream.findFirst().get();

        final Stream<RowResult> insertResStream = executeUpdate(urlOrKey, queryInsert.get(), config, log, params.toArray(new Object[params.size()]));
        final RowResult insertResult = insertResStream.findFirst().get();
        // TODO: documentare che la query SQL deve avere colonne consistenti con la query neo4j

        // TODO step 2: fare dei test in cui passo una query che interroga la tabella temporanea
        /*
        SELECT
    actor,
    genre,
    movies_count,
    RANK() OVER (PARTITION BY genre ORDER BY movies_count DESC) AS rank
        FROM temp_data
        ORDER BY genre, rank;
         */

        /* ad esempio
        WITH ranked_data AS (
            SELECT 
                category_column, 
                pivot_column, 
                value_column,
                ROW_NUMBER() OVER (PARTITION BY category_column ORDER BY value_column DESC) AS rank
            FROM neo4j_data
            )
         */
        
        // todo - altri test con query sql, tipo questo
        /*
        SELECT 
            actor,
            genre,
            movies_count,
            RANK() OVER (PARTITION BY genre ORDER BY movies_count DESC) AS rank
        FROM movies_data;
         */
        
        /*
        
         */

        // TODO  step 3: return result
        try {
            return executeQuery(urlOrKey, sqlQuery, config, log, params.toArray(new Object[params.size()]));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Make sure the SQL is consistent with Cypher query which has columns: %s", columns.get()));
        }
    }

    private String mapSqlType(Object value) {
        if (value instanceof Number) return "INTEGER";
        else return "VARCHAR";
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
