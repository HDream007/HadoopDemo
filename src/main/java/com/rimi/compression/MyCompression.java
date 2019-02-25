package com.rimi.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import sun.security.krb5.Config;

import java.io.IOException;

public class MyCompression {

    public static void main(String[] args) throws Exception {
            //压缩
       // compressor();
            //解压
        decompressor();

    }

    private static void decompressor() throws IOException {

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = codecFactory.getCodec(new Path("C:\\Users\\admin\\Desktop\\compression\\word.deflate"));
        CompressionInputStream inputstream = codec.createInputStream(fs.open(new Path("C:\\Users\\admin\\Desktop\\compression\\word.deflate")));
        FSDataOutputStream stream = fs.create(new Path("C:\\Users\\admin\\Desktop\\mapreduce\\hello.txt"));

        IOUtils.copyBytes(inputstream,stream,1024,false);
        stream.close();
        inputstream.close();

    }

    private static void compressor() throws Exception {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(configuration);
        FSDataOutputStream stream = fs.create(new Path("C:\\Users\\admin\\Desktop\\compression\\word.deflate"));

        //创建压缩编解码
        CompressionCodec codec = new DeflateCodec();
        ((DeflateCodec)codec).setConf(configuration);
        CompressionOutputStream outputStream = codec.createOutputStream(stream);

        FSDataInputStream open = fs.open(new Path("C:\\Users\\admin\\Desktop\\mapreduce\\hello.txt"));
        IOUtils.copyBytes(open,outputStream,1024,false);

        //关闭流
        outputStream.close();
        stream.close();
        open.close();
    }


}
