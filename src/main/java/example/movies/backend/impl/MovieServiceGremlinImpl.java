package example.movies.backend.impl;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import com.steelbridgelabs.oss.neo4j.structure.providers.Neo4JNativeElementIdProvider;
import example.movies.backend.interfaces.MovieService;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 30.05.12
 */
public class MovieServiceGremlinImpl implements MovieService {

    private static final Logger logger = LoggerFactory.getLogger(MovieService.class);


    private final Neo4JGraph neo4JGraph;

    public MovieServiceGremlinImpl(String uri) {
        neo4JGraph = createGremlinExecutor(uri);
    }

    private Neo4JGraph createGremlinExecutor(String uri) {
        try {
            String auth = new URL(uri.replace("bolt","http")).getUserInfo();
            if (auth != null) {
                String[] parts = auth.split(":");
                Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(parts[0], parts[1]));
                return new Neo4JGraph(driver, new Neo4JNativeElementIdProvider(), new Neo4JNativeElementIdProvider());
            }
            Driver driver = GraphDatabase.driver(uri);
            return new Neo4JGraph(driver, new Neo4JNativeElementIdProvider(), new Neo4JNativeElementIdProvider());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid Neo4j-ServerURL " + uri);
        }
    }


    @Override
    public Map findMovie(String title) {
        if (title==null) return Collections.emptyMap();
        Map<String, Object> movieData = new HashMap<>();
        List<Map> cast = new ArrayList<>();
        Vertex movie;
        try {
            movie = neo4JGraph.traversal().V().hasLabel("Movie")
                    .filter(v -> v.get().value("title").equals(title))
                    .next();
        } catch (NoSuchElementException e) {
            logger.warn(String.valueOf(e));
            return Collections.emptyMap();
        }
        movie.edges(Direction.IN).forEachRemaining(edge -> {
            Map<String, Object> actorData = new HashMap<>();
            if (edge.label().equals("ACTED_IN")) {
                actorData.put("role", edge.value("roles"));
            }
            actorData.put("name", edge.outVertex().value("name"));
            actorData.put("job", edge.label().split("_")[0].toLowerCase());
            cast.add(actorData);
        });
        movieData.put("title", title);
        movieData.put("cast", cast);
        return movieData;
    }

    public Iterable<Map<String,Object>> search(String query) {
        if (query==null || query.trim().isEmpty()) return Collections.emptyList();
        List<Vertex> movies = neo4JGraph.traversal().V().hasLabel("Movie")
                .filter(v -> v.get().value("title").toString().contains(query)).toList();
        List<Map<String, Object>> resultList = new ArrayList<>();

        movies.forEach(i -> {
            Map<String, Object> movieProps = new HashMap<>();
            Stream<VertexProperty> propertyStream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(i.properties(), Spliterator.ORDERED),
                    false);
            propertyStream.forEach(vertexProperty -> movieProps.put(vertexProperty.key(), vertexProperty.value()));
            resultList.add(Collections.singletonMap(i.label().toLowerCase(), movieProps));
        });
        return resultList;
    }


    public Map<String, Object> graph(int limit) {
        List<Map<String, Object>> movies = new ArrayList<>();
        neo4JGraph.traversal().V().hasLabel("Movie").toStream().forEach(movie -> {
            Map<String, Object> movieMap = new HashMap<>();
            List<String> actorNameList = new ArrayList<>();
            movie.edges(Direction.IN).forEachRemaining(edge -> {
                actorNameList.add(edge.outVertex().value("name"));
            });
            movieMap.put("movie", movie.value("title"));
            movieMap.put("cast", actorNameList);
            movies.add(movieMap);
        });
        Iterator<Map<String, Object>> result = movies.iterator();
        List<Map> nodes = new ArrayList<>();
        List<Map> rels= new ArrayList<>();
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

}
