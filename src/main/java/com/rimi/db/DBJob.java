package com.rimi.db;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DBJob {

    public static void main(String[] args) throws Exception{
        //获取配置信息
        Configuration configuration = new Configuration();
        //加载数据库连接驱动
        DBConfiguration.configureDB(configuration,"com.mysql.jdbc.Driver","jdbc:mysql://localhaost:3306/demo","root","123");
        //开启作业
        Job job = Job.getInstance(configuration);

        //设置map
        job.setMapperClass(DBMapper.class);
        job.setReducerClass(DBReducer.class);

        //设置map输出类型
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(IntWritable.class);

         //设置输出格式
        job.setOutputFormatClass(DBOutputFormat.class);

        DBOutputFormat.setOutput(job,"result","word","total");

        //文件输入格式
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\admin\\Disktop\\db"));

        //提交
        job.waitForCompletion(true);

    }
}