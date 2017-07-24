package com.kyle.mr.wcremote;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 
 * key values 对应mapper输出的键值对一一对应
 * 
 * @author kyle 
 *
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	/**
	 * map和reduce的输入和输出类型都是以key-alue形式封装的
	 */
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		long count = 0;
		for(LongWritable value:values){
			count += value.get();
		}
		//输出统计结果
		context.write(key, new LongWritable(count));
	}

}
