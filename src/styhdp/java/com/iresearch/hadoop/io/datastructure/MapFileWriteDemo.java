package com.iresearch.hadoop.io.datastructure;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFileWriteDemo {

	private static final String[] DATA = {
	    "One, two, buckle my shoe",
	    "Three, four, shut the door",
	    "Five, six, pick up sticks",
	    "Seven, eight, lay them straight",
	    "Nine, ten, a big fat hen"
	};
	
	public static void main(String[] args) throws IOException {
		
		String uri = args[0]; //numbers.map
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		//Path path = new Path(URI.create(uri));
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		MapFile.Writer writer = null;
				
		try {
			
			//SequenceFile.Writer构造函数参数是Path， 不是字符串uri
			writer = new MapFile.Writer(conf, fs, uri, key.getClass(), value.getClass());
			
			for(int i=0; i<100; i++){
				key.set(i+1);
				value.set(DATA[i % DATA.length]);
				writer.append(key, value);
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally{
			IOUtils.closeStream(writer);
		}
		 
	}

}
