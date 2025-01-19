package movies;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HighestRatedMoviePerUser {

    static Logger log = Logger.getLogger(HighestRatedMoviePerUser.class.getName());

    public static void main(String[] args) throws Exception {
        System.out.println("heelllo from main");
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (files.length != 2) {
            System.err.println("Usage: highestRated <ratings input> <output>");
            System.exit(2);
        }

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = Job.getInstance(conf, "Highest Rated Movie Per User");
        job.setJarByClass(HighestRatedMoviePerUser.class);
        job.setMapperClass(MapForRatings.class);
        job.setReducerClass(ReduceForRatings.class);

        job.setOutputKeyClass(IntWritable.class); // userId
        job.setOutputValueClass(Text.class);     // movieId,rating

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        boolean result = job.waitForCompletion(true);
        System.out.println("Job Status: " + result);
        System.exit(result ? 0 : 1);
    }

    public static class MapForRatings extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("heelooo");
            String[] fields = value.toString().split(",");
            if (fields[0].equals("userId")) {
                return;
            }
            
            try {
                int userId = Integer.parseInt(fields[0]); // userId
                String movieRating = fields[1] + "," + fields[2]; // movieId,rating
                context.write(new IntWritable(userId), new Text(movieRating));
            } catch (NumberFormatException e) {
                log.warning("Skipping invalid line: " + value.toString());
            }
        }
        
    }

    public static class ReduceForRatings extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double maxRating = Double.MIN_VALUE;
            String bestMovie = "";

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                String movieId = parts[0];
                double rating = Double.parseDouble(parts[1]);

                if (rating > maxRating) {
                    maxRating = rating;
                    bestMovie = movieId;
                }
            }
            context.write(key, new Text(bestMovie));
        }
    }
}
