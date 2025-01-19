package movies;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupMoviesByLikeCount {

    public static class UserMovieMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) {
                String movieName = fields[1].trim();
                context.write(new Text(movieName), one);
            }
        }
    }

    public static class MovieLikeCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get(); 
            }
            context.write(key, new IntWritable(sum)); 
        }
    }

    public static class GroupByLikeCountMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) {
                String movieName = fields[0].trim(); 
                int likeCount = Integer.parseInt(fields[1].trim());
                context.write(new IntWritable(likeCount), new Text(movieName)); 
            }
        }
    }

    public static class GroupByLikeCountReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> movies = new ArrayList<>();
            for (Text value : values) {
                movies.add(value.toString());
            }
            String result = String.join(" ", movies); 
            context.write(key, new Text(result)); 
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Movie Likes Count");
        job1.setJarByClass(GroupMoviesByLikeCount.class);
        job1.setMapperClass(UserMovieMapper.class);
        job1.setReducerClass(MovieLikeCounterReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class); 

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path intermediateOutput = new Path("/output/intermediate_output"); // Intermédiaire dans /output

        //Path intermediateOutput = new Path("intermediate_output");
        FileOutputFormat.setOutputPath(job1, intermediateOutput);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Group Movies by Like Count");
        job2.setJarByClass(GroupMoviesByLikeCount.class);
        job2.setMapperClass(GroupByLikeCountMapper.class); 
        job2.setReducerClass(GroupByLikeCountReducer.class);
        job2.setOutputKeyClass(IntWritable.class); 
        job2.setOutputValueClass(Text.class); 

        FileInputFormat.addInputPath(job2, intermediateOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}