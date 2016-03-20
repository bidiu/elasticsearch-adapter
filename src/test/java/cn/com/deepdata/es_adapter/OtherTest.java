package cn.com.deepdata.es_adapter;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import org.junit.Ignore;
import org.junit.Test;

@SuppressWarnings("unused")
public class OtherTest {
	
	@Ignore
	@Test
	public void test() throws FileNotFoundException {
		File file = new File("/Users/sunhe/Desktop/test.txt");
		new PrintStream(file).close();
	}

}
