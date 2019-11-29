package example.movies.backend.interfaces;

import java.util.Map;

public interface MovieService {

    Map findMovie(String title);
    Iterable<Map<String,Object>> search(String query);
    Map<String, Object> graph(int limit);

}
