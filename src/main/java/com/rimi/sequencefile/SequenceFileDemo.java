package com.rimi.sequencefile;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

public class SequenceFileDemo {
    public static void main(String[] args) throws IOException {
//        序列化文件
//        createSequenceFile();
        //反序列化
            ReadSequenceFile();
    }

    public static void createSequenceFile() throws IOException {

        Configuration configuration = new Configuration();

        SequenceFile.Writer.Option file = SequenceFile.Writer.file(new Path("C:\\Users\\admin\\Desktop\\seq\\word.seq"));
        SequenceFile.Writer.Option keyClass =  SequenceFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option valueClass =  SequenceFile.Writer.valueClass(Text.class);
        SequenceFile.Writer writer = SequenceFile.createWriter(configuration,file,keyClass,valueClass);

        writer.append(new IntWritable(1),new Text("hello"));
        writer.close();
    }

    public static void ReadSequenceFile() throws IOException {

        Configuration configuration = new Configuration();

        //文件选项
        SequenceFile.Reader.Option file = SequenceFile.Reader.file(new Path("C:\\Users\\admin\\Desktop\\seq\\word.seq"));

        SequenceFile.Reader reader = new SequenceFile.Reader(configuration, file);

       Writable key = new IntWritable();
       Writable value = new Text();

       //方式一
       while(reader.next(key,value)){
           System.out.println("key:"+key+", value:"+value);
       }
       /*//方式二
        while(reader.next(key)){
            reader.getCurrentValue(value);
            System.out.println(key+"===="+value);
        }*/
        reader.close();
    }


}
