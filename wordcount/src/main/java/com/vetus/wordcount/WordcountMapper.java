package com.vetus.wordcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordcountMapper extends
        Mapper<LongWritable, Text, Text, LongWritable>
{

    /**
     * The `Mapper` function. It receives a line of input from the file, 
     * extracts the words, and adds one entry foreach word with value 1
     * @param key - Input key - The line offset in the file - ignored.
     * @param value - Input Value - This is the line itself.
     * @param context - Provides access to the OutputCollector and Reporter.
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws 
            IOException, InterruptedException {

        // get all words in a line
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreElements()) {
            Text word = new Text(itr.nextToken());
            context.write(word, new LongWritable(1));
        }
    }
}
