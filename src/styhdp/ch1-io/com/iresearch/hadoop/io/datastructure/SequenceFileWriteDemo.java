package com.iresearch.hadoop.io.datastructure;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

//SequenceFile写的  测试类
public class SequenceFileWriteDemo {
	
	private static final String[] DATA = {
	    "One, two, buckle my shoe",
	    "Three, four, shut the door",
	    "Five, six, pick up sticks",
	    "Seven, eight, lay them straight",
	    "Nine, ten, a big fat hen"
	};
	
	public static void main(String[] args) throws IOException {
		// fs, conf, path, key.class, value.class
		
		String uri = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		SequenceFile.Writer writer = null;
		
		writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
		
		try {
			for(int i=0; i<100; i++){
				key.set(100-i);
				value.set( DATA[i % DATA.length] );
				System. out. printf("[%s]\t%s\t%s\n" , writer.getLength(), key, value);
				writer.append(key, value);
			}
		} catch (Exception e) {
			IOUtils.closeStream(writer);
		}
		
	}
}
