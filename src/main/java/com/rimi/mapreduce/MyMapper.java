package com.rimi.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*Text keyout = new Text();
        IntWritable valueOut = new IntWritable();

        //切割成字符串数组
        String[] s = value.toString().split(" ");
        //遍历数组
        for (String s1 : s) {
            keyout.set(s1);
            valueOut.set(1);
            context.write(keyout,valueOut);
        }*/

        String[] s = value.toString().split(" ");
        for (String s1 : s) {
            context.write(new Text(s1),new IntWritable(1));
        }
    }
}
