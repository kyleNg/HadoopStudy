package com.kyle.mr.provinceflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * 自定义的javabean需要实现序列化
 * 因为Hadoop的 map和reduce过程都需要网络通信所以必须要将对象序列化
 * @author kyle
 *
 */
public class FlowBean implements WritableComparable<FlowBean>{

	private long upFlow;
	private long dFlow;
	private long sumFlow;
	
	// 序列化的时候需要用到反射,需要一个空参的构造函数
	public FlowBean() {
	}
	
	public FlowBean(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow + dFlow;
	}
	
	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getdFlow() {
		return dFlow;
	}

	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	/**
	 * 序列化的方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
	}

	/**
	 * 反序列化方法
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		 upFlow = in.readLong();
		 dFlow = in.readLong();
		 sumFlow = in.readLong();
	}

	@Override
	public int compareTo(FlowBean o) {
		return this.sumFlow>o.getSumFlow()?-1:1;
	}

	@Override
	public String toString() {
		 
		return upFlow + "\t" + dFlow + "\t" + sumFlow;
	}
}
