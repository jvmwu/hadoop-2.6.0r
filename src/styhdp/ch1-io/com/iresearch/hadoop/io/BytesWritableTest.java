package com.iresearch.hadoop.io;
import static org.hamcrest.CoreMatchers.is;
//import static org.hamcrest.Matchers.greaterThan;
//import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import com.iresearch.hadoop.io.base.WritableTestBase;

public class BytesWritableTest extends WritableTestBase {
	
	@Test
	public void test() throws IOException{
		
		// 观察 BytesWritable 序列化的数据形式
		BytesWritable bytesWritable = new BytesWritable(new byte[]{3, 5});
		System.out.println(Arrays.toString( serialize(bytesWritable) ));     //[0, 0, 0, 2, 3, 5]
		assertThat(serializeToHexString(bytesWritable), is("000000020305"));
		
		// BytesWritable getLength()和getBytes().length 方法的区别
		bytesWritable.setCapacity(10);                                           
		assertThat( bytesWritable.getLength(), is(2) );                      //2
		System.out.println(Arrays.toString( serialize(bytesWritable) ));     //[0, 0, 0, 2, 3, 5]
		assertThat( bytesWritable.getBytes().length, is(10) );               //10
		
		bytesWritable.setCapacity(1);
		assertThat( bytesWritable.getLength(), is(1) );                      //1
		System.out.println(Arrays.toString( serialize(bytesWritable) ));     //[0, 0, 0, 1, 3]
		assertThat( bytesWritable.getBytes().length, is(1) );		         //1
		/*
		  public BytesWritable(byte[] bytes) {
		    this.bytes = bytes;
		    this.size = bytes.length;
		  }		
          public void setCapacity(int new_cap) {
            if (new_cap != getCapacity()) {
              byte[] new_data = new byte[new_cap];
              if (new_cap < size) {
                size = new_cap;
              }
              if (size != 0) {
                System.arraycopy(bytes, 0, new_data, 0, size);
              }
              bytes = new_data;
            }
          }
		*/
	}
}
