package com.iresearch.hadoop.io;
import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import com.iresearch.hadoop.io.base.WritableTestBase;

public class ObjectWritableTest extends WritableTestBase {

	public static void main(String[] args) throws IOException {
		
		Text text = new Text("\u0041");
		ObjectWritable writable = new ObjectWritable(text);
		System.out.println( StringUtils.byteToHexString( serialize(writable)) );
		//00196f72672e6170616368652e6861646f6f702e696f2e5465787400196f72672e6170616368652e6861646f6f702e696f2e546578740141
		//(a)0019 6f72672e6170616368652e6861646f6f702e696f2e54657874， (b)0019 6f72672e6170616368652e6861646f6f702e696f2e54657874，(c)0141
		/*
          (1)序列化  ObjectWritable 的声明部分
             UTF8.writeString(out, declaredClass.getName());  ==>
             
		     0019 6f72672e6170616368652e6861646f6f702e696f2e54657874(第一部分是一个short数值，为该对象class名字的字符串长度，org.apache.hadoop.io.Text，25位=0x0019)
		  (2)序列化 Writable 接口对象的实现类
		     if (Writable.class.isAssignableFrom(declaredClass)) { // Writable接口实现类
                UTF8.writeString(out, instance.getClass().getName());
                ((Writable)instance).write(out);
             }                                                ==>
             
             0019 6f72672e6170616368652e6861646f6f702e696f2e54657874
             0141(可变长Text的序列化值，0x01长度，0x41数值内容)
		 */
		
		ObjectWritable srcWritable = new ObjectWritable(Integer.TYPE, 188);
		ObjectWritable destWritable = new ObjectWritable();
		cloneInto(srcWritable, destWritable);
		System.out.println( serializeToHexString(srcWritable) ); //0003696e74000000bc
		System.out.println((Integer)destWritable.get());         //188
	}
	
    /*	public static void writeObject(DataOutput out, Object instance, Class declaredClass, Configuration conf) throws IOException {

		if (instance == null) { // null
			instance = new NullInstance(declaredClass, conf);
			declaredClass = Writable.class;
		}

		UTF8.writeString(out, declaredClass.getName()); // always write declared

		if (declaredClass.isArray()) { // array
			int length = Array.getLength(instance);
			out.writeInt(length);
			for (int i = 0; i < length; i++) {
				writeObject(out, Array.get(instance, i),
						declaredClass.getComponentType(), conf);
			}

		} else if (declaredClass == String.class) { // String
			UTF8.writeString(out, (String) instance);

		} else if (declaredClass.isPrimitive()) { // primitive type

			if (declaredClass == Boolean.TYPE) { // boolean
				out.writeBoolean(((Boolean) instance).booleanValue());
			} else if (declaredClass == Character.TYPE) { // char
				out.writeChar(((Character) instance).charValue());
			} else if (declaredClass == Byte.TYPE) { // byte
				out.writeByte(((Byte) instance).byteValue());
			} else if (declaredClass == Short.TYPE) { // short
				out.writeShort(((Short) instance).shortValue());
			} else if (declaredClass == Integer.TYPE) { // int
				out.writeInt(((Integer) instance).intValue());
			} else if (declaredClass == Long.TYPE) { // long
				out.writeLong(((Long) instance).longValue());
			} else if (declaredClass == Float.TYPE) { // float
				out.writeFloat(((Float) instance).floatValue());
			} else if (declaredClass == Double.TYPE) { // double
				out.writeDouble(((Double) instance).doubleValue());
			} else if (declaredClass == Void.TYPE) { // void
			} else {
				throw new IllegalArgumentException("Not a primitive: "
						+ declaredClass);
			}
		} else if (declaredClass.isEnum()) { // enum
			UTF8.writeString(out, ((Enum) instance).name());
		} else if (Writable.class.isAssignableFrom(declaredClass)) { // Writable
			UTF8.writeString(out, instance.getClass().getName());
			((Writable) instance).write(out);

		} else {
			throw new IOException("Can't write: " + instance + " as "
					+ declaredClass);
		}
	}*/
}
