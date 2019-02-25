package com.rimi.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;


public class MyJob {
    public static void main(String[] args) throws Exception {

        //写法一
//       method1();
        //写法二
        // method2();

        Configuration cf = new Configuration();
//        如果要从windows系统中运行这个job提交客户端程序，需添加跨平台提交参数
        cf.set("mapreduce.app-submission.cross-platform","true");

        FileSystem fs = FileSystem.get(new URI("hdfs:hd01"),cf,"root");
        //获取文件输出路径
        String outPath = args[1];

        if (fs.exists(new Path(outPath))){
            fs.delete(new Path(outPath),true);
        }

        //设置本地模式
        //cf.set("fs.defaultFS","file:///");

        //开启一个作业
        Job job = Job.getInstance(cf);

        //设置作业名称
        job.setJobName("wordcount");
        job.setJarByClass(MyJob.class);
        job.setJar("C:\\Users\\admin\\IdeaProjects\\hadoopdemo\\target\\hadoop-demo-1.0-SNAPSHOT.jar");

        FileInputFormat format = new TextInputFormat();
        //设置最大切片
        format.setMaxInputSplitSize(job,50);
        //format.setMinInputSplitSize();

        //设置文件输出压缩方式
        FileOutputFormat.setCompressOutput(job,true);
        //设置压缩编码 解码方式
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);



        //设置输入的格式
        job.setInputFormatClass(TextInputFormat.class);
        //设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置输出数据的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置map和reduce
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        //设置reduce的个数
        job.setNumReduceTasks(1);

        //设置输入文件地址  和  输出文件地址
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交作业
        job.waitForCompletion(true);

    }









    /*private static void method2() throws Exception {
        Configuration conf = new Configuration();
        //开启一个作业
        Job job = Job.getInstance(conf);
        //设置程序入口
        job.setJarByClass(MyJob.class);

        //指定map
        job.setMapperClass(MyMapper.class);
        //设置输出的 key / value
        job.setMapOutputKeyClass(Text.class); //map的key值为String 类型  ---> hadoop  Text
        job.setMapOutputValueClass(IntWritable.class);  //value 为 int 类型  ---> hadoop Intwritable

        //指定reduce
        job.setReducerClass(MyReduce.class);

        //指定需要分析的文件路径
        FileInputFormat.addInputPath(job,new Path("C:\\Users\\admin\\Desktop\\mapreduce\\hello.txt"));

        //判断输出的路径下  是否存在文件或目录
        Path outpath = new Path("C:\\Users\\admin\\Desktop\\mapreduce\\out");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)){
            fs.delete(outpath,true);
        }

        //指定输出文件路径
        FileOutputFormat.setOutputPath(job,outpath);

        //提交作业
        boolean b = job.waitForCompletion(true);
        if (b){
            System.out.println("提交成功");
        }else {
            System.out.println("提交失败");
        }
    }

    private static void method1() throws  Exception{
        Configuration cf = new Configuration();

        //获取文件输出路径

        //设置本地模式
        //cf.set("fs.defaultFS","file:///");

        //开启一个作业
        Job job = Job.getInstance(cf);

        //设置作业名称
        job.setJobName("wordcount");
        job.setJarByClass(MyJob.class);

        //设置输入的格式
        job.setInputFormatClass(TextInputFormat.class);
        //设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置输出数据的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置map和reduce
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        //设置reduce的个数
        job.setNumReduceTasks(1);

        //设置输入文件地址  和  输出文件地址
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\admin\\Desktop\\mapreduce\\hello.txt"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\admin\\Desktop\\mapreduce\\out"));

        //提交作业
        job.waitForCompletion(true);
    }*/
}
