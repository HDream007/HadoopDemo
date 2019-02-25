package com.rimi.SortSecend;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class SortJob {
    public static void main(String[] args) throws Exception {
        //获取配置信息
        Configuration configuration = new Configuration();
        //开启作业
        Job job = Job.getInstance(configuration);

        //设置程序入口
        job.setJobName("SortJob");
        job.setJarByClass(SortJob.class);

        //设置map 和 reduce
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        //设置 map输出
        job.setMapOutputKeyClass(CombinerKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置输出类型


    }
}
