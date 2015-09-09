package com.iresearch.hadoop.io;

import java.io.IOException;
//import java.util.EmptyStackException;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import com.iresearch.hadoop.io.base.WritableTestBase;

public class GenericWritableTest extends WritableTestBase{
	
	public static void main(String[] args) throws IOException {
		Text text = new Text("hadoop");
		MyGenericWritable writable = new MyGenericWritable(text);
		
		System.out.println(StringUtils.byteToHexString( serialize(text) ));     //066861646f6f70
		System.out.println(StringUtils.byteToHexString( serialize(writable) )); //00066861646f6f70 ==> 00，066861646f6f70(org.apache.hadoop.io.Text在classes的第一位)
		
		writable.set(new BytesWritable(new byte[]{3,5}));
		System.out.println(serializeToHexString(writable));                     //01000000020305   ==> 01，000000020305(org.apache.hadoop.io.BytesWritable在classes的第二位)
		System.out.println( ((BytesWritable)writable.get()).toString() );       //03 05
		System.out.println( writable.toString() );                              //GW[class=org.apache.hadoop.io.BytesWritable,value=03 05]
		
		/*
          //GenericWritable 对象的set(Writable obj)方法，重置 instance 和 type 的值
		  public void set(Writable obj) {
		    instance = obj;
		    Class<? extends Writable> instanceClazz = instance.getClass();
		    Class<? extends Writable>[] clazzes = getTypes();
		    for (int i = 0; i < clazzes.length; i++) {
		      Class<? extends Writable> clazz = clazzes[i];
		      if (clazz.equals(instanceClazz)) {
		        type = (byte) i;
		        return;
		      }
		    }
		    throw new RuntimeException("The type of instance is: " + instance.getClass() + ", which is NOT registered.");
		  }
		  
		  //GenericWritable 序列化方法
		  public void write(DataOutput out) throws IOException {
		    if (type == NOT_SET || instance == null)
		      throw new IOException("The GenericWritable has NOT been set correctly. type=" + type + ", instance=" + instance);
		    out.writeByte(type); //这里type值等于  需要包装的对象在 MyGenericWritable.classes 中的索引位置
		    instance.write(out);
		  }

		*/
	}
}


@SuppressWarnings("unchecked")
class MyGenericWritable extends GenericWritable {
	
	public MyGenericWritable(Writable writable){
		set(writable);
	}
	
	public static Class<? extends Writable>[] classes = null;

	static {
		classes = (Class<? extends Writable>[])new Class[]{
			Text.class, BytesWritable.class
		};
	}
	
	@Override
	protected Class<? extends Writable>[] getTypes() {
		return classes;
	}
	
}

// Class<? extends Writable>[] classess = (Class<? extends Writable>[])new Class[]{Text.class};

/*class TestGenericArray <E>{
	private E[] elements;
	private int size = 0;
	private static final int DEFAULT_INITIAL_CAPACITY = 16;
	
	public TestGenericArray(){
		elements = new E[DEFAULT_INITIAL_CAPACITY]; //Cannot create a generic array of E
	}
	
	public E pop(){
		if(size ==0){
			throw new EmptyStackException();
		}
		E result = elements[--size];
		elements[size] = null;
		return result;
	}
}*/

/*class TestGenericArray <E>{
	private E[] elements;
	private int size = 0;
	private static final int DEFAULT_INITIAL_CAPACITY = 16;
	
	@SuppressWarnings("unchecked")
	public TestGenericArray(){
		elements = (E[])new Object[DEFAULT_INITIAL_CAPACITY]; //Cannot create a generic array of E
	}
	
	public E pop(){
		if(size ==0){
			throw new EmptyStackException();
		}
		E result = elements[--size];
		elements[size] = null;
		return result;
	}
}*/

/*class TestGenericArray<E>{
	private Object[] elements;
	private int size = 0;
	private static final int DEFAULT_INITIAL_CAPACITY = 16;
	
	public TestGenericArray(){
		elements = new Object[DEFAULT_INITIAL_CAPACITY]; //Cannot create a generic array of E
	}
	
	public E pop(){
		if(size ==0){
			throw new EmptyStackException();
		}
		@SuppressWarnings("unchecked")
		E result = (E)elements[--size];
		elements[size] = null;
		return result;
	}
}*/