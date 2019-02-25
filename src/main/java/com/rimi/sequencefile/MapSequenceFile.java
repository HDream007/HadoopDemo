package com.rimi.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MapSequenceFile {
    public static void main(String[] args) throws IOException {
        read();
        //
        //writer();
    }

    public static void writer() throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path("C:\\Users\\admin\\Desktop\\seq\\map");

        MapFile.Writer.Option option = MapFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option valueClass = MapFile.Writer.valueClass(Text.class);
        MapFile.Writer writer = new MapFile.Writer(configuration,path,option,valueClass);

        for (int i = 0; i < 100; i++) {
            writer.append(new IntWritable(i),new Text("hello world ！！"));
//            writer.append(new IntWritable(i),new Text("hello world ！！"));
        }
        writer.close();

    }


    public static void read() throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path("C:\\Users\\admin\\Desktop\\seq\\map.seq");
        MapFile.Reader reader = new MapFile.Reader(path,configuration);
        IntWritable key = new IntWritable();
        Text value = new Text();

        reader.seek(new IntWritable(68));


        while( reader.next(key,value)){
            System.out.println(key+"=="+value);
        }
        reader.close();
    }
}
