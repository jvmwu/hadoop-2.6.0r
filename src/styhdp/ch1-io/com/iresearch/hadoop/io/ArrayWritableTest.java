package com.iresearch.hadoop.io;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.iresearch.hadoop.io.base.WritableTestBase;

public class ArrayWritableTest extends WritableTestBase {
	
	@Test
	public void testArrayWritable() throws IOException{
		
		ArrayWritable arrayWritable = new ArrayWritable(Text.class);
		arrayWritable.set(new Text[]{new Text("hadoop"), new Text("hive")});
		
		//先写入表示数组长度的int值，最后依次写入序列化后的存储对象值 0002，6 1049700111111112，4 104105118101
		System.out.println( Arrays.toString(serialize(arrayWritable)) ); //[0, 0, 0, 2, 6, 104, 97, 100, 111, 111, 112, 4, 104, 105, 118, 101]
		
		/*
		  
		  public void readFields(DataInput in) throws IOException {
		    values = new Writable[in.readInt()];          // construct values
		    for (int i = 0; i < values.length; i++) {
		      Writable value = WritableFactories.newInstance(valueClass);
		      value.readFields(in);                       // read a value
		      values[i] = value;                          // store it in values
		    }
		  }
		
		  public void write(DataOutput out) throws IOException {
		    out.writeInt(values.length);                 // write values
		    for (int i = 0; i < values.length; i++) {
		      values[i].write(out);
		    }
		  }
		
		*/
		
		MyArrayWritable myWritable = new MyArrayWritable();
		cloneInto(arrayWritable, myWritable);
		assertThat(myWritable.get().length, is(2));
		assertThat((Text)myWritable.get()[0], is(new Text("hadoop")));
		
		//测试 ArrayWritable 的toArray()方法
		Text[] textArray = (Text[])myWritable.toArray();
		System.out.println(textArray[1].toString());  //hive
	}
}

class MyArrayWritable extends ArrayWritable{

	public MyArrayWritable() {
		super(Text.class);
	}
	
}
