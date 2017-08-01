package com.kyle.mr.provinceflow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * K2  V2  对应的是map输出kv的类型
 * 
 * @author kyle
 *
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

	public static HashMap<String, Integer> proviceDict = new HashMap<String, Integer>();
	static {
		proviceDict.put("136", 0);
		proviceDict.put("137", 1);
		proviceDict.put("138", 2);
		proviceDict.put("139", 3);
	}

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {

		String profix = key.toString().substring(0, 3);
		Integer provinceID = proviceDict.get(profix);
		return provinceID == null ? 4 : provinceID;
	}

}
