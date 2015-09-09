package com.iresearch.hadoop.io.datastructure;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

//SequenceFile读的  测试类
public class SequenceFileReadDemo {
	
	//private static final String FS_DEFAULT = "hdfs://hadoop-master:9000";
	
	public static void main(String[] args) throws IOException {
		
		String uri = args[0];
		Configuration configuration = new Configuration();
		
		FileSystem fs = FileSystem.get(URI.create(uri), configuration);
		Path path = new Path(uri);
		
		SequenceFile.Reader reader = null;
		try {
			
			//get SequenceFile Reader
			reader = new SequenceFile.Reader(fs, path, configuration);
			
			//get key and value Object
			Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
			Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), configuration);
			
			//read the key and value
			long position = reader.getPosition();
			while(reader.next(key, value)){
				String syncSeen = reader.syncSeen()? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition();
			}
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}finally{
			IOUtils.closeStream(reader);
		}

	}

}
