package cn.com.deepdata.es_adapter.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cn.com.deepdata.es_adapter.Pipeline;
import cn.com.deepdata.es_adapter.PipelineFactory;

/**
 * Utility that helps you adapt multiple files in a
 * same directory.
 * <p/>
 * You assign the root directory, and the file name
 * suffix (typically extension name), then this utility
 * class will find all of the file with the suffix name
 * assigned by you, and adapt them.
 */
public class DirectoryAdaptUtil {

	protected PipelineFactory pipelineFactory;

	protected String suffix;

	protected List<File> xlsFileList = new ArrayList<File>();

	public DirectoryAdaptUtil(PipelineFactory pipelineFactory, String rootDir, String suffix) {
		this.pipelineFactory = pipelineFactory;
		this.suffix = suffix;
		findXlsFiles(new File(rootDir));
	}

	protected void findXlsFiles(File dir) {
		File[] xlsFiles = dir.listFiles(new FilenameFilter() {

			public boolean accept(File dir, String name) {
				if (name.endsWith(suffix)) {
					return true;
				}
				else {
					return false;
				}
			}

		});
		xlsFileList.addAll(new ArrayList<File>(Arrays.asList(xlsFiles)));

		File[] innerDirs = dir.listFiles(new FileFilter() {

			public boolean accept(File pathname) {
				if (pathname.isDirectory()) {
					return true;
				}
				else {
					return false;
				}
			}

		});
		for (File innerDir : innerDirs) {
			findXlsFiles(innerDir);
		}
	}

	protected void transASingleXlsFile(File file) throws InterruptedException {
		System.out.println("transfering file: " + file.getAbsolutePath());
		Pipeline pipeline = pipelineFactory.getInstance();
		pipeline.putData(file);
		pipeline.close();
		System.out.println("done: " + file.getAbsolutePath());
	}

	public void start() throws InterruptedException {
		for (File xlsFile : xlsFileList) {
			transASingleXlsFile(xlsFile);
		}
	}

}
