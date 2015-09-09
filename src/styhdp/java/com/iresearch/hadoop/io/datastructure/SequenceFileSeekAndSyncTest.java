package com.iresearch.hadoop.io.datastructure;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SequenceFileSeekAndSyncTest {
	
	private final static String SF_URI = "numbers.seq";
	
	@Test  //test case 1
	public void seekToRecordBoundary() throws IOException{
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(SF_URI), conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(SF_URI), conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
	    reader.seek(359);
	    assertThat(reader.next(key, value), is(true));
	    assertThat( ((IntWritable)key).get(), is(95));
	    
	}
	
	@Test(expected=IOException.class) //test case 2
	public void seekToNonRecordBoundary() throws IOException{
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(SF_URI), conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(SF_URI), conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
	    reader.seek(360);
	    reader.next(key, value);
	}
	
	@Test
	public void syncFromNonRecordBoundary() throws IOException{
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(SF_URI), conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(SF_URI), conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		reader.sync(360);
	    assertThat(reader.getPosition(), is(2021L));
	    assertThat(reader.next(key, value), is(true));
	    assertThat(((IntWritable) key).get(), is(59));
	}
	
	@Test
	public void syncAfterLastSyncPoint() throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(SF_URI), conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(SF_URI), conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		reader.sync(4557);
		assertThat(reader.getPosition(), is(4788L));
		assertThat(reader.next(key, value), is(false));
	}
	
}
