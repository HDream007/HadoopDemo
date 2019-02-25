package com.rimi.db;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DBReducer extends Reducer<Text, IntWritable, Result, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        Result result = new Result();
        result.setWord(key.toString());
        result.setTotal(sum);
        context.write(result,NullWritable.get());
    }
}
