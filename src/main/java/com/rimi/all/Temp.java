package com.rimi.all;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class Temp {
    public static void main(String[] args) throws Exception {
        File file = new File("C:\\Users\\admin\\Disktop\\tem[\\temp.txt");
        FileOutputStream outputStream = new FileOutputStream(file,true);

        //创建序列文件书写器
        SequenceFile.createWriter(new Configuration(),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(IntWritable.class),
                SequenceFile.Writer.file(new Path("C:\\Users\\admin\\Disktop\\tem[\\temp\\temp.seq")));

    }
}
