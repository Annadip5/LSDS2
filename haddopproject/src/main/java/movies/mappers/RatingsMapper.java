package movies.mappers;
import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 3) {
            String userId = fields[0];
            String movieId = fields[1];
            String rating = fields[2];
            context.write(new Text(userId), new Text(movieId + "," + rating));
        }
    }
}
