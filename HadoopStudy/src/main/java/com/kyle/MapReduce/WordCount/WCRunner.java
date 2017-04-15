package com.kyle.MapReduce.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 用来描诉特定的作业
 * 比如,该作业使用哪个类作为逻辑处理中的map哪个是reduce
 * 还可以指定要处理的数据在哪
 * 作业输出结果放在哪
 * @author kyle
 *
 */
public class WCRunner {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job wcJob = Job.getInstance(conf);
		//设置整个job所用的类在哪个jar
		wcJob.setJarByClass(WCRunner.class);
		
		//本job使用的map和reduce类
		wcJob.setMapperClass(WCMapper.class);
		wcJob.setReducerClass(WCReducer.class);
		
		//指定reduce的输出数据类型KV类型
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(LongWritable.class);
		
		//指定要处理数据存放路径
		FileInputFormat.setInputPaths(wcJob, new Path("hdfs://Hadoop-Server:9000//wc/word.txt"));
		
		//指定处理结果的输出数据存放路径
		FileOutputFormat.setOutputPath(wcJob, new Path("hdfs://Hadoop-Server:9000//wc/output/"));
		
		//将job提交给集群运行
		wcJob.waitForCompletion(true);
	}
}






















