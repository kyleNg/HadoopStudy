package com.kyle.HadoopStudy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Created by kyle on 17/03/30.
 */
public class HdoopHdfs {
	
    static FileSystem fs = null;
    
    public void init() throws IOException, InterruptedException, URISyntaxException{
    	//读取classpath下的xxx-site.xml配置文件，并解析其内容，封装到conf对象中
    	Configuration conf = new Configuration();
    	//也可以在代码中对conf配置信息进行手动设置，将会覆盖掉配置文件
//    	conf.set("fs.defaultFS", "hdfs://Hadoop-Server:9000/");
    	//根据配置信息，去获取一个具体的系统客户端操作系统对象
//    	fs = FileSystem.get(new URI("hdfs://Hadoop-Server:9000/"), conf, "root");
    	fs = FileSystem.get(conf);
    }
    
    static{
    	//读取classpath下的xxx-site.xml配置文件，并解析其内容，封装到conf对象中
    	Configuration conf = new Configuration();
    	//也可以在代码中对conf配置信息进行手动设置，将会覆盖掉配置文件
//    	conf.set("fs.defaultFS", "hdfs://Hadoop-Server:9000/");
    	//根据配置信息，去获取一个具体的系统客户端操作系统对象
//    	fs = FileSystem.get(new URI("hdfs://Hadoop-Server:9000/"), conf, "root");
    	try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /**
     * 
     * 上传文件
     * 
     * @param src 本地要上传的文件路径
     * @param dst hdfs上面要上传的路径
     * @throws IOException
     */
    public static void upload(Path src, Path dst) throws IOException{
    	fs.copyFromLocalFile(src, dst);
    }
    
    /**
     * 
     * 下载文件
     * 
     * @param src 要下载的hdfs路径
     * @param dst 下载存放的本地路径
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void dowonlod(Path src, Path dst) throws URISyntaxException, IOException, InterruptedException {
    	//复制文件
        fs.copyToLocalFile(src,dst);
    }
    
    /**
     * 创建文件夹
     * 
     * @param path 想要创建的文件夹
     * @throws IOException
     */
    public static void mkdir(Path path) throws IOException{
    	fs.mkdirs(path);
    	
    }
    
    
    /**
     * 查看文件信息
     * 
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException{
    	//listfiles 列出文件信息,并提供遍历
    	RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
    	while(files.hasNext()){
    		LocatedFileStatus file = files.next();
    		Path filePath = file.getPath();
    		String fileName = filePath.getName();
    		System.out.println(fileName);
    	}
    	System.out.println("----------------------------");
    	//listStatus可以列出文件和文件夹信息,并不提供自带的递归
    	FileStatus[] listStatus = fs.listStatus(new Path("/"));
    	for (FileStatus status : listStatus) {
			String name = status.getPath().getName();
			System.out.println(name+(status.isDirectory()?" is dir":" is file"));
		}

    }
    
    public static void rm(Path path) throws IOException{
    	fs.delete(path,true);
    }
    
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
    	Path src = new Path("/home/testuser/testfile");
    	Path dst = new Path("hdfs://Hadoop-Server:9000//testfile");
    	//上传文件测试
//    	upload(src,dst);
    	
    	src = new Path(new URI("hdfs://Hadoop-Server:9000//dockerfile/docker2.pdf"));
    	dst = new Path("/home/testuser");
    	//下载文件测试
//    	dowonlod(src, dst);
    	
    	//创建文件家测试
//    	mkdir(new Path("hdfs://Hadoop-Server:9000//mkdir"));
    	
    	//看文件信息测试
//    	listFiles();
    	
//    	rm(new Path("/dockerfile"));
    	
    	
    }
}
