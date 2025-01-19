package movies.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class BestMovieReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> bestMovies = new ArrayList<>();
        double maxRating = Double.MIN_VALUE;

        for (Text value : values) {
            String[] fields = value.toString().split(",");
            String movieId = fields[0];
            double rating = Double.parseDouble(fields[1]);
            if (rating > maxRating) {
                maxRating = rating;
                bestMovies.clear(); 
                bestMovies.add(movieId);
            } else if (rating == maxRating) {
                bestMovies.add(movieId); 
            }
        }

        String bestMovie = bestMovies.get((int) (Math.random() * bestMovies.size()));

        context.write(key, new Text(bestMovie));

    }
}
