package cn.com.deepdata.es_adapter.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.elasticsearch.action.index.IndexResponse;

import cn.com.deepdata.es_adapter.Pipeline;
import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings.SettingsBuilder;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.listener.ResponseListener;

/**
 * The file to be processed should end with ".csv" 
 * extension name.
 * <p/>
 * This class is thread-safe.
 * 
 * @author sunhe
 * @date Mar 20, 2016
 */
public class CsvFileProcessor {
	
	public static final String DEFAULT_DELIMITER = ",";
	public static final String MAPPER_FILE_EXTENSION_NAME = "mapper";
	public static final String MARK_DONE_EXTENSION_NAME = "done";
	
	private PipelineSettings settings;
	private SettingsBuilder builder;
	private AdapterChainInitializer initializer;
	private ResponseListener<IndexResponse> listener;
	private List<String> keyList;
	
	private File file;
	private String charset;
	private Pattern delimiter;
	private boolean hasTitleLine;
	/**
	 * If this attribute is true, then all of the CSV files 
	 * within same folder should share a single mapper file, whose 
	 * filename doesn't matter (as long as it has '.mapper' extension). 
	 * However, you have to make sure that 
	 * there is one and only one mapper file in the folder, otherwise 
	 * exception will occur..
	 */
	private boolean shareSingleMapper;
	private boolean extractIndexTypeName;
	
	/**
	 * Note that setting's index and type make no effect.
	 * 
	 * @param file
	 * @param charset
	 * @param delimiter
	 * @param hasTitleLine
	 * @param settings
	 * @author sunhe
	 * @date Mar 21, 2016
	 */
	public CsvFileProcessor(File file, String charset, String delimiter, boolean hasTitleLine, 
			SettingsBuilder builder, boolean shareSingleMapper, boolean extractIndexTypeName, 
			AdapterChainInitializer initializer, ResponseListener<IndexResponse> listener) {
		this.builder = builder;
		keyList = new ArrayList<String>();
		this.file = file;
		this.charset = charset;
		this.delimiter = Pattern.compile("\\s*" + delimiter + "\\s*");
		this.hasTitleLine = hasTitleLine;
		this.shareSingleMapper = shareSingleMapper;
		this.extractIndexTypeName = extractIndexTypeName;
		this.initializer = initializer;
		this.listener = listener;
	}
	
	private boolean isDone() {
		return FilenameUtil.getBrotherFile(
				file, FilenameUtil.changeExtensionName(file.getName(), MARK_DONE_EXTENSION_NAME)) != null;
	}
	
	private void markDone() throws FileNotFoundException {
		File doneFile = FilenameUtil.createBrotherFile(
				file, FilenameUtil.changeExtensionName(file.getName(), MARK_DONE_EXTENSION_NAME));
		PrintStream out = null;
		try {
			out = new PrintStream(doneFile);
		}
		finally {
			if (out != null) {
				out.close();
			}
		}
	}
	
	/**
	 * 
	 * @param filename
	 * 		mapper file's filename
	 * @author sunhe
	 * @date Mar 21, 2016
	 */
	private void extractIndexType(String filename) {
		if (! extractIndexTypeName) {
			settings = builder.build();
			return;
		}
		
		filename = FilenameUtil.excludeExtensionName(filename);
		String index = filename.substring(0, filename.indexOf("-"));
		String type = filename.substring(filename.indexOf("-") + 1);
		settings = builder.index(index).type(type).build();
	}
	
	private void extractKeyList() throws FileNotFoundException {
		// TODO refine pattern
		File[] files = FilenameUtil.getBrotherFiles(file, 
				shareSingleMapper ? 
				Pattern.compile("^.+\\." + MAPPER_FILE_EXTENSION_NAME + "$") :
				Pattern.compile("^\\w+-\\w+\\." + MAPPER_FILE_EXTENSION_NAME + "$"));
		if (files.length == 0) {
			throw new FileNotFoundException("Cannot find the mapper file corresponding to " + file);
		}
		else if (files.length > 1) {
			throw new IllegalStateException("Find multiple mapper files corresponding to " + file);
		}
		else {
			extractIndexType(files[0].getName());
		}
		
		Scanner fileScanner = null;
		Scanner lineScanner = null;
		try {
			fileScanner = new Scanner(files[0], charset);
			lineScanner = new Scanner(fileScanner.nextLine());
			lineScanner.useDelimiter(delimiter);
			while (lineScanner.hasNext()) {
				keyList.add(lineScanner.next().trim());
			}
		}
		finally {
			if (fileScanner != null) {
				fileScanner.close();
			}
			if (lineScanner != null) {
				lineScanner.close();
			}
		}
	}
	
	private void skipTitleLine(Scanner scanner) {
		if (hasTitleLine) {
			scanner.nextLine();
		}
	}
	
	/**
	 * TODO IndexOutOfBoundException <br/>
	 * TODO Add support for asynchronous execution <br/>
	 * 
	 * @throws FileNotFoundException
	 * @throws InterruptedException
	 * @author sunhe
	 * @date Mar 20, 2016
	 */
	public synchronized void process() throws FileNotFoundException, InterruptedException {
		if (isDone()) {
			System.out.println("File's already been processed, so skip " + file);
			return;
		}
		
		Scanner filescanner = null;
		Scanner lineScanner = null;
		Pipeline pipeline = null;
		Map<String, Object> doc = null;
		
		try {
			extractKeyList();
			pipeline = Pipeline.build(settings, initializer, listener);
			filescanner = new Scanner(file, charset);
			skipTitleLine(filescanner);
			
			while (filescanner.hasNextLine()) {
				try {
					lineScanner = new Scanner(filescanner.nextLine());
					lineScanner.useDelimiter(delimiter);
					doc = new HashMap<String, Object>();
					for (int i = 0; lineScanner.hasNext(); i++) {
						String val = lineScanner.next().trim();
						if (val.equals("")) {
							val = null;
						}
						doc.put(keyList.get(i), val);
					}
					pipeline.putData(doc);
				}
				catch (IndexOutOfBoundsException e) {
					// do nothing
				}
			}
			
			markDone();
		}
		finally {
			if (filescanner != null) {
				filescanner.close();
			}
			if (lineScanner != null) {
				lineScanner.close();
			}
			if (pipeline != null) {
				pipeline.close();
			}
		}
	}
	
}
