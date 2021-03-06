package com.vetus.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App 
{
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: wordcount <input path> <output path>");
            System.exit(-1);
        }

        // Create the job specification object
        Job job = new Job();
        job.setJarByClass(App.class);
        job.setJobName("Word count");

        // Setup input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the Mapper and Reducer classes
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WorkcountReducer.class);

        // Specify the type of output keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Wait for the job to finish before terminating
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
