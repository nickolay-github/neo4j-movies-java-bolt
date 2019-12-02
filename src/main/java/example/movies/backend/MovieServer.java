package example.movies.backend;

import example.movies.backend.impl.MovieServiceCypherImpl;
import example.movies.backend.impl.MovieServiceGremlinImpl;
import example.movies.backend.interfaces.MovieService;
import example.movies.util.Util;

import static spark.Spark.*;

/**
 * @author Michael Hunger @since 22.10.13
 */
public class MovieServer {

    public static void main(String[] args) {
        port(Util.getWebPort());
        externalStaticFileLocation("src/main/webapp");
        final MovieService service = new MovieServiceGremlinImpl(Util.getNeo4jUrl());
        new MovieRoutes(service).init();
    }
}
