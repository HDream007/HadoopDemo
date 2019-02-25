package com.rimi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Scanner;

public class MapReduce {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws Exception {


//    访问远程文件系统的四种方式
        
//        方式一： 通过URL连接远程文件系统
        demo01();
//          方式二：通过URI获取远程文件系统
        demo02();
//            方式三：设置配置信息
        demo03();
//        方式四：加载core-site.xml配置文件
        demo04();
        
        //远程文件系统添加文件
        addFile();
        //删除文件系统中的文件
        deleteFile();
    }

    private static void deleteFile() throws Exception {
        //获取配置信息
        Configuration configuration = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hd01"),configuration,"root");
        //删除指定文件
        boolean b = fs.delete(new Path("/hello1.txt"), false);
        if (!b){
            System.out.println("删除失败");
        }
        System.out.println("删除成功");
    }

    private static void addFile() throws Exception{
        //获取配置信息
        Configuration configuration = new Configuration();
        //获取远程文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hd01"),configuration,"root");
        //创建文件输入流
        FileInputStream inputStream = new FileInputStream("C:\\Users\\admin\\Desktop\\hello1.txt");
        //创建文件输出流
        FSDataOutputStream outputStream = fs.create(new Path("/hello1.txt"), false, 1024, (short) 2, 256 * 1024 * 1024);
        //设置权限
        fs.setPermission(new Path("hello1.txt"),new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
        //关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
    }

    private static void demo04() throws Exception{
        //创建配置文件
        Configuration configuration = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(configuration);
        //创建文件输入流
        FSDataInputStream inputStream = fs.open(new Path("/anaconda-ks1.cfg"));
        //将输入流写到输出流
        IOUtils.copyBytes(inputStream,System.out,1024,false);
        //关闭流
        IOUtils.closeStream(inputStream);
    }

    private static void demo03() throws Exception {
        //创建配置信息
        Configuration configuration = new Configuration();
        //设置远程连接地址
        configuration.set("fs.defaultFS","hdfs://hd01");
        //获取文件系统
        FileSystem fs = FileSystem.get(configuration);
        //创建文件输入流
        FSDataInputStream inputStream = fs.open(new Path("/anaconda-ks1.cfg"));
        //把输入流写到输出流
        IOUtils.copyBytes(inputStream,System.out,1024,false);
        //关闭资源
        IOUtils.closeStream(inputStream);
    }

    private static void demo02() throws Exception {
        //创建配置信息
        Configuration configuration = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hd01"),configuration,"root");
        //创建文件输入流
        FSDataInputStream inputStream = fs.open(new Path("/anaconda-ks1.cfg"));
        //把输入流写到输出流
        IOUtils.copyBytes(inputStream,System.out,1024,false);
        //关闭资源
        IOUtils.closeStream(inputStream);

    }

    private static void demo01() throws Exception {
        //读取文件
        InputStream stream = new URL("hdfs://hd01/anaconda-ks1.cfg").openStream();   
        Scanner scanner = new Scanner(stream);
        while(scanner.hasNext()){
            String s1 = scanner.nextLine();
            System.out.println(s1);
        }
        
    }


}
