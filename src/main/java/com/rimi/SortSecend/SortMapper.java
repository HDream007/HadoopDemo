package com.rimi.SortSecend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,CombinerKey, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        CombinerKey combinerKey = new CombinerKey();
        combinerKey.setYear(Integer.valueOf(split[0]));
        combinerKey.setTemp(Integer.valueOf(split[1]));
        context.write(combinerKey,NullWritable.get());
    }
}
