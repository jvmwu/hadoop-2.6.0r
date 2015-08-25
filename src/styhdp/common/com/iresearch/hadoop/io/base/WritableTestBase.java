package com.iresearch.hadoop.io.base;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class WritableTestBase {
	
	/**
	 * 将一个实现了 org.apache.hadoop.io.Writable 接口的对象序列化成字节流
	 * 
	 * @param writable
	 * @return byte[]
	 * @throws java.io.IOException
	 */
	public static byte[] serialize(Writable writable) throws IOException {

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

		writable.write(dataOutputStream);
		if (null != dataOutputStream) {
			dataOutputStream.close();
		}

		return byteArrayOutputStream.toByteArray();
	}

	/**
	 * 将字节流转换为实现了 org.apache.hadoop.io.Writable 接口的对象
	 * 
	 * @param writable
	 * @return byte[]
	 * @throws java.io.IOException
	 */
	public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
		DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

		writable.readFields(dataInputStream);
		if (null != dataInputStream) {
			dataInputStream.close();
		}

		return bytes;
	}
	
	/**
	 * 将一个实现了 org.apache.hadoop.io.Writable 接口的对象序列化成字节流, 并返回该字节流的 十六进制 字符串形式
	 * 
	 * @param writable
	 * @return String
	 * @throws java.io.IOException
	 */
	public static String serializeToHexString(Writable writable) throws IOException{
		return StringUtils.byteToHexString( serialize(writable) );
	}
	
	/**
	 * 将 一个实现  Writable 接口对象的数据 写入 到另外一个  实现 Writable 接口的对象中
	 * 
	 * @param Writable src  源对象
	 * @param Writable dest 待写入目标对象
	 * @return 待写入字节流的 十六进制 字符串 形式
	 * @throws java.io.IOException
	 */
	public static String cloneInto(Writable src, Writable dest) throws IOException{
		byte[] bytes = deserialize(dest, serialize(src));
		return StringUtils.byteToHexString( bytes );
	}
}
