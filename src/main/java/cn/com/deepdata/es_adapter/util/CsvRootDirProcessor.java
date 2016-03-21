package cn.com.deepdata.es_adapter.util;

import java.io.File;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;

/**
 * 
 * @author sunhe
 * @date 2016年3月21日
 */
public class CsvRootDirProcessor {
	
	private File rootDir;
	private String charset;
	private boolean hasTitleLine;
	private PipelineSettings settings;
	
	public CsvRootDirProcessor(String rootDir, String charset, boolean hasTitleLine, PipelineSettings settings) {
		this(new File(rootDir), charset, hasTitleLine, settings);
	}
	
	public CsvRootDirProcessor(File rootDir, String charset, boolean hasTitleLine, PipelineSettings settings) {
		this.rootDir = rootDir;
		this.charset = charset;
		this.hasTitleLine = hasTitleLine;
		this.settings = settings;
	}
	
	public void process() {
		
	}
	
}
