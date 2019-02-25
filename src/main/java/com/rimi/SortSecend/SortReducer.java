package com.rimi.SortSecend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<CombinerKey, IntWritable, Text,IntWritable> {
    @Override
    protected void reduce(CombinerKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = 0 ;
        for (IntWritable value : values) {
            int temp = value.get();
            max = max > temp ? max : temp;
        }
        context.write(new Text(key.getYear().toString()),new IntWritable(max));
    }
}
