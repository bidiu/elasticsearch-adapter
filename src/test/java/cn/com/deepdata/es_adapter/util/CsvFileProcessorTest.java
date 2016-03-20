package cn.com.deepdata.es_adapter.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;

import org.junit.Ignore;
import org.junit.Test;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;

@SuppressWarnings("unused")
public class CsvFileProcessorTest {
	
	@Ignore
	@Test
	public void test() throws FileNotFoundException, InterruptedException {
		File file = new File("/Users/sunhe/Desktop/test/library21-book.csv");
		PipelineSettings settings = PipelineSettings.getDefaultSettings();
		CsvFileProcessor processor = new CsvFileProcessor(file, "UTF-8", true, settings);
		processor.process();
	}
	
}
