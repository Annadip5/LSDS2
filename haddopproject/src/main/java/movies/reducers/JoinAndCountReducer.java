package movies.reducers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class JoinAndCountReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String movieName = "";
        Set<String> userSet = new HashSet<>(); 

        for (Text value : values) {
            String[] fields = value.toString().split(",");
            if (fields[0].equals("MOVIE")) {
                movieName = fields[1];
            } else if (fields[0].equals("USER")) {
                userSet.add(fields[1]); 
            }
        }

        context.write(new Text(String.valueOf(userSet.size())), new Text(movieName));

    }
}

