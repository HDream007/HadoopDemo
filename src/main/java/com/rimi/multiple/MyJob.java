package com.rimi.multiple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;


public class MyJob {
    public static void main(String[] args) throws Exception {
        //创建配置信息
        Configuration configuration = new Configuration();
        //开启一个作业
        Job job = Job.getInstance(configuration);

        //设置作业名称
        job.setJobName("WordCount");
        //设置程序入口
        job.setJarByClass(MyJob.class);
        //指定运行的jar包
        job.setJar("C:\\Users\\admin\\IdeaProjects\\hadoopdemo\\target\\hadoop-demo-1.0-SNAPSHOT.jar");

        //设置输入输出格式
        job.setInputFormatClass(TextInputFormat.class);
    }
}
