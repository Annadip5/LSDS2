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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JoinHighestRatedMovie {

    public static class MoviesMapper extends Mapper<Object, Text, IntWritable, Text> {

        private static final Pattern CSV_PATTERN = Pattern.compile("^([0-9]+),\"?(.*?)\"?,(.*)$");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim(); // Suppression des espaces inutiles

            if (line.contains("\"") && (line.split("\"").length % 2 != 0)) {
                System.err.println("Ligne ignorée (guillemets non appariés) : " + line);
                return;
            }

            Matcher matcher = CSV_PATTERN.matcher(line);
            if (matcher.matches()) {
                try {
                    int movieId = Integer.parseInt(matcher.group(1));
                    String movieTitle = matcher.group(2).trim();
                    String genres = matcher.group(3).trim();

                    // Vérification que toutes les parties sont présentes
                    if (!movieTitle.isEmpty() && !genres.isEmpty()) {
                        // Écrire dans le contexte avec le format attendu
                        context.write(new IntWritable(movieId), new Text("M:" + movieTitle));
                    } else {
                        // Log si certaines parties sont manquantes
                        System.err.println("Ligne ignorée (champs manquants) : " + line);
                    }
                } catch (NumberFormatException e) {
                    // Log si le movieId n'est pas un entier valide
                    System.err.println("Erreur de parsing du movieId dans la ligne : " + line);
                }
            } else {
                // Log en cas de ligne malformée
                System.err.println("Ligne ignorée (malformée) : " + line);
            }
        }
    }





    public static class RatingsMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (fields.length == 2) {
                int userId = Integer.parseInt(fields[0]); // userId
                int movieId = Integer.parseInt(fields[1]); // movieId
                context.write(new IntWritable(movieId), new Text("U:" + userId));

            }
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String movieName = null; 
            List<String> movieNames = new ArrayList<>(); 

            for (Text value : values) {
                String record = value.toString();
                if (record.startsWith("M:")) {
                    movieName = record.substring(2);
                    movieNames.add(movieName);
                }
            }

            if (!movieNames.isEmpty()) {
                StringBuilder movieList = new StringBuilder();
                for (String movie : movieNames) {
                    movieList.append(movie).append(" ");
                }
                context.write(key, new Text(movieList.toString().trim()));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("hhjjjkjk");
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (files.length != 3) {
            System.err.println("Usage: JoinHighestRatedMovie <movies input> <ratings input> <output>");
            System.exit(2);
        }

        Path moviesInput = new Path(files[1]);
        Path ratingsInput = new Path(files[0]);
        Path output = new Path(files[2]);

        Job job = Job.getInstance(conf, "Join Highest Rated Movie");
        job.setJarByClass(JoinHighestRatedMovie.class);
        MultipleInputs.addInputPath(job, ratingsInput, TextInputFormat.class, RatingsMapper.class);
        MultipleInputs.addInputPath(job, moviesInput, TextInputFormat.class, MoviesMapper.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
