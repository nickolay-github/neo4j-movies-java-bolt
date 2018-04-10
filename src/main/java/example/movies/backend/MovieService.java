package example.movies.backend;

import example.movies.executor.BoltCypherExecutor;
import example.movies.executor.CypherExecutor;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static java.util.Collections.singletonMap;

/**
 * @author mh
 * @since 30.05.12
 */
public class MovieService {

    private final CypherExecutor cypher;

    public MovieService(String uri) {
        cypher = createCypherExecutor(uri);
    }

    private CypherExecutor createCypherExecutor(String uri) {
        try {
            String auth = new URL(uri.replaceFirst("bolt(\\+routing)?","http")).getUserInfo();
            if (auth != null) {
                String[] parts = auth.split(":");
                return new BoltCypherExecutor(uri,parts[0],parts[1]);
            }
            return new BoltCypherExecutor(uri);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid Neo4j-ServerURL " + uri);
        }
    }

    public Map findMovie(String title) {
        if (title==null) return Collections.emptyMap();
        return cypher.query(
                "MATCH (movie:Movie {title:{title}})" +
                " OPTIONAL MATCH (movie)<-[r]-(person:Person)\n" +
                " RETURN movie.title as title, collect({name:person.name, job:head(split(lower(type(r)),'_')), role:r.roles}) as cast LIMIT 1",
                singletonMap("title", title)).stream().findFirst().orElse(null);
    }

    @SuppressWarnings("unchecked")
    public Iterable<Map<String,Object>> search(String query) {
        if (query==null || query.trim().isEmpty()) return Collections.emptyList();
        return cypher.query(
                "MATCH (movie:Movie)\n" +
                " WHERE lower(movie.title) CONTAINS {part}\n" +
                " RETURN movie",
                singletonMap("part", query.toLowerCase()));
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> graph(int limit) {
        Iterator<Map<String,Object>> result = cypher.query(
                "MATCH (m:Movie)<-[:ACTED_IN]-(a:Person) " +
                " RETURN m.title as movie, collect(a.name) as cast " +
                " LIMIT {limit}", singletonMap("limit",limit)).iterator();
        List nodes = new ArrayList();
        List rels= new ArrayList();
        int i=0;
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            nodes.add(map("title",row.get("movie"),"label","movie"));
            int target=i;
            i++;
            for (Object name : (Collection) row.get("cast")) {
                Map<String, Object> actor = map("title", name,"label","actor");
                int source = nodes.indexOf(actor);
                if (source == -1) {
                    nodes.add(actor);
                    source = i++;
                }
                rels.add(map("source",source,"target",target));
            }
        }
        return map("nodes", nodes, "links", rels);
    }

    private static Map<String,Object> map(Object...keyValues) {
        Map<String, Object> result = new LinkedHashMap<>(keyValues.length / 2);
        for (int i = 0; i < keyValues.length; i+=2) {
            result.put(keyValues[i].toString(),keyValues[i+1]);
        }

        return result;
    }
}
