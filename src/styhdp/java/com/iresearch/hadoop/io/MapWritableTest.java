package com.iresearch.hadoop.io;

import static org.hamcrest.CoreMatchers.is;
//import static org.hamcrest.Matchers.greaterThan;
//import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.junit.Test;

import com.iresearch.hadoop.io.base.WritableTestBase;

public class MapWritableTest extends WritableTestBase {
	
	//测试 MapWritable 中的key
	@Test
	public void testKeyInMapWritable() throws IOException{
		MapWritable mapWritable = new MapWritable();
		mapWritable.put(new IntWritable(1), new Text("hadoop"));
		mapWritable.put(new VIntWritable(2), new BytesWritable(new byte[]{3,5}));
		
		MapWritable destWritable = new MapWritable();
		cloneInto(mapWritable, destWritable);
		assertThat((Text)destWritable.get(new IntWritable(1)), is(new Text("hadoop")));
		
		/*
		  assertThat( ((BytesWritable)destWritable.get(new IntWritable(2))).getLength(), is(2)); 
		  ==> 出错，java.lang.NullPointException，说明  MapWritable 是以键的class 类型存储，和实际Writable对象值无关系
		  
		  MapWritable构造函数源码如下：
		  
		    public Writable put(Writable key, Writable value) {
               addToMap(key.getClass());
               addToMap(value.getClass());
               return instance.put(key, value);
            }
            
                          其中addToMap(class clazz)为父类 AbstractMapWritable 的方法：
            
            protected synchronized void addToMap(Class clazz) {
               if (classToIdMap.containsKey(clazz)) { return; }
               if (newClasses + 1 > Byte.MAX_VALUE) {
                 throw new IndexOutOfBoundsException("adding an additional class would exceed the maximum number allowed");
               }
               byte id = ++newClasses;
               addToMap(clazz, id);
            }
            
            Map<Class, Byte> classToIdMap = new ConcurrentHashMap<Class, Byte>();
            Map<Byte, Class> idToClassMap = new ConcurrentHashMap<Byte, Class>();
            //继承 AbstractMapWritable的MapWritable和SortedMapWritable最多可以使用127个不同的非标准 Writable 类
            private volatile byte newClasses = 0;  
            
            private synchronized void addToMap(Class clazz, byte id) {
               if (classToIdMap.containsKey(clazz)) {
                  byte b = classToIdMap.get(clazz);
                  if (b != id) {
                     throw new IllegalArgumentException ("Class " + clazz.getName() + " already registered but maps to " + b + " and not " + id);
                  }
               }
               if (idToClassMap.containsKey(id)) {
                  Class c = idToClassMap.get(id);
                  if (!c.equals(clazz)) {
                     throw new IllegalArgumentException("Id " + id + " exists but maps to " + c.getName() + " and not " + clazz.getName());
                  }
               }
               classToIdMap.put(clazz, id);
               idToClassMap.put(id, clazz);
            }
                          
		*/
		assertThat( ((BytesWritable)destWritable.get(new VIntWritable(2))).getLength(), is(2));
	}
	
	//测试 MapWritable 的序列化过程，序列化过程见 图3.2
	@Test
	public void testSerialize() throws IOException{
		
		MapWritable mapWritable = new MapWritable();
		mapWritable.put(new IntWritable(1), new Text("hadoop"));
		mapWritable.put(new VIntWritable(2), new BytesWritable(new byte[]{3,5}));
		
		//000000000285000000018c066861646f6f708e0283000000020305
		//00, 00000002, 85, 00000001, 8c, 06 6861646f6f70, 8e, 02, 83, 00000002 35
		System.out.println(serializeToHexString(mapWritable));
	}
}