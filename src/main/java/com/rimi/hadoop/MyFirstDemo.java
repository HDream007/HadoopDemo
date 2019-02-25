package com.rimi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Scanner;

public class MyFirstDemo {

    static{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws Exception {
//       常规方式访问远程文件系统
//       Demo01();
        //配置远程地址方式
//         Demo02();
//         Demo03();
         //Demo04();

//        将本地文件写到hdfs中
//            Write();
            //删除hdfs中指定的文件
//            Delete();
            //遍历文件
            ShowDirectory();
    }

    private static void ShowDirectory() throws Exception{
        //创建配置信息
        Configuration configuration = new Configuration();
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hd01"),configuration,"root");
        //查找文件
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("文件上一次访问时间："+fileStatus.getAccessTime());
            System.out.println("文件的拥有者："+fileStatus.getOwner());
            System.out.println("文件的分组："+fileStatus.getGroup());
            System.out.println("文件的权限："+fileStatus.getPermission());
        }

    }

    private static void Delete() throws Exception {
        //创建配置信息
        Configuration configuration = new Configuration();
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hd01"), configuration,"root");
        //删除文件
        boolean delete = fileSystem.delete(new Path("/user"), false);
        System.out.println(delete);

    }

    private static void Write() throws Exception{
        //获取配置信息
        Configuration configuration = new Configuration();
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hd01"), configuration,"root");
        //创建文件输入流
        FileInputStream fileInputStream = new FileInputStream("C:\\Users\\admin\\Desktop\\hello1.txt");
        //创建文件输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hello1.txt"), false, 1024, (short) 2, 256 * 1024 * 1024);
        //设置权限
        fileSystem.setPermission(new Path("/hello1.txt"),new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
        //关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(fileInputStream);
    }

    /**
     * 加载core-site.xml文件的方式
     * @throws IOException
     */
    private static void Demo04() throws IOException {
        //创建配置文件
        Configuration configuration = new Configuration();
        //获取文件系统
        FileSystem system = FileSystem.get(configuration);
        //创建文件输入流
        FSDataInputStream open = system.open(new Path("/anaconda-ks1.cfg"));
        //把文件输入流写到输出流
        IOUtils.copyBytes(open,System.out,1024,false);
        //关闭流
        IOUtils.closeStream(open);

    }

    //给配置信息设置
    private static void Demo03() throws IOException {
        //创建配置信息
        Configuration configuration = new Configuration();
        //设置远程连接地址
        configuration.set("fs.defaultFS","hdfs://hd01");
        //获取文件系统
        FileSystem fs = FileSystem.get(configuration);
        //创建文件输入流
        FSDataInputStream open = fs.open(new Path("/anaconda-ks1.cfg"));
        //把文件输入流写到输入流
        IOUtils.copyBytes(open,System.out,1024,false);
        //关闭流
        IOUtils.closeStream(open);

    }

    private static void Demo02() throws IOException {
       //配置远程地址方式一：
        //创建配置信息
        Configuration configuration = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hd01"),configuration);
        //创建文件输入流
        FSDataInputStream open = fs.open(new Path("/anaconda-ks1.cfg"));
        //把输入流写到输出流
        IOUtils.copyBytes(open,System.out,1024,false);
        //关闭流
        IOUtils.closeStream(open);

    }

    public static void Demo01() throws IOException {
        //读取文件
        InputStream stream = new URL("hdfs://hd01/anaconda-ks1.cfg").openStream();
        Scanner scanner = new Scanner(stream);
        while(scanner.hasNext()){
            String s = scanner.nextLine();
            System.out.println(s);
        }
    }


}
