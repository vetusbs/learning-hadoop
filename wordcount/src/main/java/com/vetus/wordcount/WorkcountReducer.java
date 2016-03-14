package com.vetus.wordcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;



public class WorkcountReducer extends
        Reducer<Text, LongWritable, Text, LongWritable>
{

    /**
     * The `Reducer` function. Iterates over all the word entries, and sum them
     * @param key - Input key - word
     * @param values - Input Value - one
     * @param context - Used for collecting output
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public void reduce(Text key, Iterable<LongWritable> values,
            Context context) throws IOException, InterruptedException {

        Long result = 0L;
        for (LongWritable value : values) {
            result = value.get() + result ;
        }
        
        context.write(key, new LongWritable(result));
    }
}
