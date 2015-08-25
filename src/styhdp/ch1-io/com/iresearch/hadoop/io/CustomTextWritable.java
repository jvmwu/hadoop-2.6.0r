package com.iresearch.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import com.iresearch.hadoop.io.base.WritableTestBase;

public class CustomTextWritable implements WritableComparable<CustomTextWritable> {
	
	private Text first;
	private Text second;
	
	public CustomTextWritable(){
		set(new Text(), new Text());
	}
	
	public CustomTextWritable(Text first, Text second){
		set(first, second);
	}
	
	public CustomTextWritable(String first, String second){
		set(new Text(first), new Text(second));
	}
	
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public Text getFirst(){
		return first;
	}
	
	public Text getSecond(){
		return second;
	}
	
	public byte[] getBytes(){
		return ArrayUtils.addAll(first.getBytes(), second.getBytes());
	}
	
	public int getLength(){
		return first.getLength() + second.getLength();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof CustomTextWritable){
			return first.equals( ((CustomTextWritable)obj).first ) && second.equals( ((CustomTextWritable)obj).second );
		}
		return false;
	}

	@Override
	public String toString() {
		return first.toString() + "\t" +second.toString();
	}

	@Override
	public int compareTo(CustomTextWritable other) {
		int cmp = first.compareTo(other.first);
		if(cmp != 0){
			return cmp;
		}
		return second.compareTo(other.second);
	}
	
	// the default comparator of CustomTextWritable
	public static class Comparator extends WritableComparator{
		
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		protected Comparator() {
			super(CustomTextWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			
			try {
				//WritableUtils.decodeVIntSize(b1[s1]) 表示 first数据存储长度   数值的字节长度
				//readVInt(b1, s1) 表示 first数据存储字节长度
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				
				//compare the first field,  ERROR ==> int cmp = TEXT_COMPARATOR.compare(b1, s1, l1, b2, s2, l2); 
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
				
				if(cmp != 0){
					return cmp;
				}
				
				// first field is same, then compare the second field.
				return TEXT_COMPARATOR.compare(b1, s1+firstL1, l1-firstL1, b2, s2+firstL2, l2-firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
			
		}
		
	}
	
	static{
		WritableComparator.define(CustomTextWritable.class, new Comparator());
	}
	
	//A custom RawComparator for comparing the first field of CustomTextWritable
	public static class FirstComparator extends WritableComparator{
		
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		protected FirstComparator() {
			super(CustomTextWritable.class);
		}
		
		//序列化后，字节的直接比较方法
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				
				return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			}
		}
		
		//在非序列化时，对象的比较方法
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			
			if(a instanceof CustomTextWritable && b instanceof CustomTextWritable){
				return ( ((CustomTextWritable)a).first.compareTo( ((CustomTextWritable)b).first) );
			}
			return super.compare(a, b);
		}
	}
}

class MainTest extends WritableTestBase{
	
	public static void main(String[] args) throws IOException {
		
		CustomTextWritable writableA = new CustomTextWritable("hadoop","hive");
		CustomTextWritable writableB = new CustomTextWritable("hadoop","hive");
		
		@SuppressWarnings("unchecked")
		RawComparator<CustomTextWritable> comparator = WritableComparator.get(CustomTextWritable.class);
		//int compare = comparator.compare(writableA, writableB);
		
		byte[] bytesA = serialize(writableA);
		byte[] bytesB = serialize(writableB);
		int compare = comparator.compare(bytesA, 0, bytesA.length, bytesB, 0, bytesB.length);
		
		System.out.println(signum(compare));
		
	}
	
	public static int signum(int a){
		return (a<0)? -1 : ( (a==0)?0:1 );
	}
}