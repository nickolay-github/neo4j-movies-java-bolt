package example.movies;

import example.movies.backend.impl.MovieServiceCypherImpl;
import example.movies.backend.interfaces.MovieService;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author mh
 * @since 14.04.16
 */
public class DocTest {

    @Test
    public void testMovieFind() throws Exception {
        MovieService service = new MovieServiceCypherImpl("bolt://neo4j:123@localhost");
        Map movie = service.findMovie("The Matrix");
        assertEquals("The Matrix", movie.get("title"));
        assertNotNull(movie.get("cast"));
    }
}
