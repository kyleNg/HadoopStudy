package com.kyle.mr.wcremote;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 4个泛型前两个是指定mapper的输入类,KEYIN是输入的key的值类型,VALUEIN是输入的alue类型
 * 
 * map和reduce的输入和输出类型都是以key-alue形式封装的
 * 
 * @author kyle
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	//每读一行数据就调用一次该方法
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//具体业务逻辑就写在这个方法体中
		//key是这一行的偏移量 value是这一行的内容
		String line = value.toString();
		String[] words = StringUtils.split(line, " ");
		for (String word : words) {	
			context.write(new Text(word), new LongWritable(1));
		}
	}
}










