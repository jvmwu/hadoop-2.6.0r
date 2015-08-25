package com.iresearch.hadoop.io;

import static org.hamcrest.CoreMatchers.is;
//import static org.hamcrest.Matchers.greaterThan;
//import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.iresearch.hadoop.io.base.WritableTestBase;

public class TextTest extends WritableTestBase {
	
	@Test
	public void test() throws IOException{
		
		Text text =new Text("hadoop");
		
		//getLength(), getBytes().length
		assertThat( text.getLength(), is(6) );
		assertThat( text.getBytes().length, is(6) );
		
		//charAt()
		System.out.println(text.charAt(0)); 
		assertThat( text.charAt(0), is((int) 'h') );
		assertThat("Out of bounds..", text.charAt(100), is(-1) );
		
		//find()
		assertThat("find a substring from Text", text.find("do"), is(2));
		assertThat("find first 'o' ", text.find("o"), is(3));
		assertThat("find 'p' from position 5 ", text.find("p",5), is(5));
		assertThat("No match", text.find("hive"), is(-1));
		
		System.out.println(serializeToHexString(text)); //066861646f6f70
	}
	
	//part1: Text 与 java.lang.String 区别之 索引
	@Test
	public void testIndex() throws UnsupportedEncodingException{
		Text text =new Text("\u0041\u00DF\u6771\uD801\uDC00");
		String str = new String("\u0041\u00DF\u6771\uD801\uDC00");
		
		// ^^ step 1: Test for String
		assertThat(str.length(), is(5));
		assertThat(str.getBytes("UTF-8").length, is(10));
		  //indexOf(String str)
		assertThat(str.indexOf("\u0041"), is(0));
		assertThat(str.indexOf("\u00DF"), is(1));
		assertThat(str.indexOf("\u6771"), is(2));
		assertThat(str.indexOf("\uD801"), is(3));
		assertThat(str.indexOf("\uDC00"), is(4));
		  //charAt(int index)
		assertThat(str.charAt(0), is('\u0041'));
		assertThat(str.charAt(1), is('\u00DF'));
		assertThat(str.charAt(2), is('\u6771'));
		assertThat(str.charAt(3), is('\uD801'));
		assertThat(str.charAt(4), is('\uDC00'));		
		  //codePointAt(int index)
		assertThat(str.codePointAt(0), is(0x0041));
		assertThat(str.codePointAt(1), is(0x00DF));
		assertThat(str.codePointAt(2), is(0x6771));
		assertThat(str.codePointAt(3), is(0x10400)); // \uD801\uDC00, 这里表示的是一个Unicode编码
		// vv step 1: Test for String
		
		// ^^ step 2: Test for Text
		assertThat(text.getLength(), is(10));
		  //find(String str) 找到该字符的 UTF-8二进制编码在Text对象的字节偏移量
		assertThat(text.find("\u0041"), is(0));
		assertThat(text.find("\u00DF"), is(1));
		assertThat(text.find("\u6771"), is(3));
		assertThat(text.find("\uD801"), is(-1)); //在Text对象的 UTF-8编码中 '\uD801\uDC00'是相当于一个候补字符
		assertThat(text.find("\uDC00"), is(-1));
		assertThat(text.find("\uD801\uDC00"), is(6));
		
		  //charAt(int position)
		assertThat(text.charAt(0), is(0x0041));  //该方法和 java.lang.String的codePointAt(int index)类似
		assertThat(text.charAt(1), is(0x00DF));
		assertThat(text.charAt(2), is(-1));
		assertThat(text.charAt(3), is(0x6771));
		assertThat(text.charAt(6), is(0x10400));
		
		System.out.println(text.getLength());       //10
		System.out.println(text.getBytes().length); //11
		/*
		 * [position,limit,capacity] ===> [0, 10, 11]
		 * 
		 *	  public Text(String string) {
		 *	    set(string);
		 *	  }
		 *	  public void set(String string) {
		 *	    try {
		 *	      ByteBuffer bb = encode(string, true);
		 *	      bytes = bb.array();
		 *	      length = bb.limit();
		 *	    }catch(CharacterCodingException e) {
		 *	      throw new RuntimeException("Should not have happened " + e.toString()); 
		 *	    }
		 *	  }
		 */
		System.out.println(str.length());           //5
		// vv step 2: Test for Text
	}
	
	//part2: 对Text对象的遍历
	@Test
	public void testForEachText(){
		Text text =new Text("\u0041\u00DF\u6771\uD801\uDC00");
		
		ByteBuffer buffer = ByteBuffer.wrap(text.getBytes(), 0, text.getLength());
		int mark;
		while( buffer.hasRemaining() && (mark=Text.bytesToCodePoint(buffer))!= -1 ) {
			System.out.println(Integer.toHexString(mark));
		}
		//41
		//df
		//6771
		//10400
	}
	
	//part3: Text的易变性
	@Test
	public void testMutability(){
		
		Text text = new Text("hadoop");            // ==> [104, 97, 100, 111, 111, 112]
		
		/* Text 易变性的测试，与所有的Writable接口实现相似，NullWritable除外 */
		//text.set("hive");
		//System.out.println(text.getLength());       ==>4
		//System.out.println(text.getBytes().length); ==>4
		
		/* getBytes()方法返回的字节数组长度可能比getLength()长 */
		text.set(new Text("hive"));                // ==> [104, 105, 118, 101]
		System.out.println(text.getLength());      // ==>4
		System.out.println(text.getBytes().length);// ==>6 长度不变 , bytes=[104, 105, 118, 101, 111, 112]
	    
		/*
		  public void set(String string) {
		    try {
		      ByteBuffer bb = encode(string, true);
		      bytes = bb.array();
		      length = bb.limit();
		    }catch(CharacterCodingException e) {
		      throw new RuntimeException("Should not have happened " + e.toString()); 
		    }
		  }

		  public void set(Text other) {
		    set(other.getBytes(), 0, other.getLength());
		  }
		
		  public void set(byte[] utf8, int start, int len) {
		    setCapacity(len, false); 
		      //将utf8[104, 105, 118, 101]字节数组，长度为4覆盖到bytes[104, 97, 100, 111, 111, 112]中 ,结果为[104, 105, 118, 101, 111, 112]
		    System.arraycopy(utf8, start, bytes, 0, len);  
		    this.length = len;  //此时this.length = len = 4 
		  } 
		*/
	}
}
