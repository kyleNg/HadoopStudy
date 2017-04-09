package com.kyle.HadoopStudy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Created by kyle on 17/03/30.
 */
public class DownlodeFile {
    static FileSystem fs = null;
    public static void dowonlod() throws URISyntaxException, IOException, InterruptedException {

        Configuration con = new Configuration();

        con.set("fs.defaultFS","hdfs://Hadoop-Server:9000/");

        fs = FileSystem.get(new URI("hdfs://Hadoop-Server:9000/"),con);

        fs.copyToLocalFile(new Path("hdfs://Hadoop-Server:9000//dockerfile/docker2.pdf"),
                new Path("/home/testuser"));
    }
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        dowonlod();
    }
}
