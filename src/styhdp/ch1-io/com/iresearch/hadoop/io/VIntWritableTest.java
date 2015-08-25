package com.iresearch.hadoop.io;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.junit.Test;

import com.iresearch.hadoop.io.base.WritableTestBase;

public class VIntWritableTest extends WritableTestBase {

	@Test
	public void testSerialize() throws IOException{
		
		VIntWritable vint = new VIntWritable(-259); 
		byte[] bytes = serialize(vint);
		System.out.println( serializeToHexString(vint) ); //860102, 2byte
		
		VIntWritable vintNew = new VIntWritable();
		deserialize(vintNew, bytes);
		
		System.out.println( vintNew.get() ); //-259
		
	    System.out.println( serializeToHexString(new VIntWritable(1)) );  //01, 1byte
	    System.out.println( serializeToHexString(new VIntWritable(-112)) ); //90, 1byte
	    System.out.println( serializeToHexString(new VIntWritable(127)) );  //7f, 1byte
	    System.out.println( serializeToHexString(new VIntWritable(128)) );  //8f80, 2byte
	    System.out.println( serializeToHexString(new VIntWritable(163)) );  //8fa3, 2byte
	    System.out.println( serializeToHexString(new VIntWritable(Integer.MAX_VALUE)) ); //8c7fffffff, 5byte
	    System.out.println( serializeToHexString(new VIntWritable(Integer.MIN_VALUE)) ); //847fffffff, 5byte
	    
	    
	    assertThat(serializeToHexString(new VLongWritable(1)), is("01")); // 1 byte
	    assertThat(serializeToHexString(new VLongWritable(127)), is("7f")); // 1 byte
	    assertThat(serializeToHexString(new VLongWritable(128)), is("8f80")); // 2 byte
	    assertThat(serializeToHexString(new VLongWritable(163)), is("8fa3")); // 2 byte
	    assertThat(serializeToHexString(new VLongWritable(Long.MAX_VALUE)), is("887fffffffffffffff")); // 9 byte
	    assertThat(serializeToHexString(new VLongWritable(Long.MIN_VALUE)), is("807fffffffffffffff")); // 9 byte
	}
	
}
