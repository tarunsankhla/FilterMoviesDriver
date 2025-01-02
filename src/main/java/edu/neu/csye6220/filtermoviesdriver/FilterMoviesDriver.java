/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package edu.neu.csye6220.filtermoviesdriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * @author tarun
 */
public class FilterMoviesDriver {

    public static void main(String[] args)  throws Exception {
        System.out.println("Starting Filter Movies with Ratings Above Threshold");
         if (args.length != 2) {
            System.err.println("Usage: FilterMoviesDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create a new Configuration
        Configuration conf = new Configuration();

        // Create a new Job
        Job job = Job.getInstance(conf, "Filter Movies By Rating");
        job.setJarByClass(FilterMoviesDriver.class);

        // Specify Mapper and Reducer classes
        job.setMapperClass(FilterMoviesMapper.class);
        job.setReducerClass(FilterMoviesReducer.class);

        // Set the output key and value types for the Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set the output key and value types for the Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Specify input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for its completion
        boolean jobComplete = job.waitForCompletion(true);

        if (jobComplete) {
            System.out.println("Job completed successfully");
        } else {
            System.err.println("Job failed");
        }

        System.exit(jobComplete ? 0 : 1);
    }
}


class FilterMoviesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text movieId = new Text();
    private IntWritable rating = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length == 4) {
            try {
                movieId.set(parts[1]); // Movie ID
                rating.set((int) (Float.parseFloat(parts[2]) * 10)); // Scaled
                context.write(movieId, rating);
            } catch (NumberFormatException e) {
                System.err.println("Error parsing rating for line: " + value.toString());
            }
        } else {
            System.err.println("Invalid line format: " + value.toString());
        }
    }
}

class FilterMoviesReducer extends Reducer<Text, IntWritable, Text, Text> {
    private static final float RATING_THRESHOLD = 4.0f;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Reducer setup started");
        super.setup(context);
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        int sum = 0, count = 0;
        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }
        float avgRating = (float) sum / count / 10; // Scale back
        if (avgRating >= RATING_THRESHOLD) {
            context.write(key, new Text("Average Rating: " + avgRating));
            System.out.println("Reducer output for movie ID: " + key.toString() + ", Average Rating: " + avgRating);
        } else {
            System.out.println("Movie ID: " + key.toString() + " filtered out with Average Rating: " + avgRating);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Reducer execution time for key " + key.toString() + ": " + (endTime - startTime) + " ms");
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Reducer cleanup completed");
        super.cleanup(context);
    }
}   