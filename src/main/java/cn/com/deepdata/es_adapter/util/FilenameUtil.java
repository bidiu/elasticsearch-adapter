package cn.com.deepdata.es_adapter.util;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

/**
 * This is class is thread-safe.
 * 
 * @author sunhe
 * @date Mar 20, 2016
 */
public class FilenameUtil {
	
	/**
	 * 
	 * @param filename
	 * 		Not include path name.
	 * @return
	 * 		extension name, empty string if it has no one.
	 * @author sunhe
	 * @date Mar 20, 2016
	 */
	public static String getExtensionName(String filename) {
		int indexOfLastDot = filename.lastIndexOf(".");
		if (indexOfLastDot > 0) {
			return filename.substring(indexOfLastDot + 1);
		}
		else {
			return "";
		}
	}
	
	/**
	 * 
	 * @param filename
	 * 		Not include path name.
	 * @return
	 * 		Apart from extension name, also not include 
	 * 		dot.
	 * @author sunhe
	 * @date Mar 20, 2016
	 */
	public static String excludeExtensionName(String filename) {
		int indexOfLastDot = filename.lastIndexOf(".");
		if (indexOfLastDot > 0) {
			return filename.substring(0, filename.lastIndexOf("."));
		}
		else {
			return filename;
		}
	}
	
	/**
	 * 
	 * @param filename
	 * 		Origin filename, not include path name.
	 * @param extensionName
	 * 		Not include dot.
	 * @return
	 * 		Not include the path name.
	 * @author sunhe
	 * @date Mar 20, 2016
	 */
	public static String changeExtensionName(String filename, String extensionName) {
		return excludeExtensionName(filename) + "." + extensionName;
	}
	
	/**
	 * A brother file is a file that 
	 * resides in the same directory.
	 * 
	 * 
	 * @param file
	 * 		current file
	 * @param pattern
	 * 		Not include path name. Note this is a regular expression, 
	 * 		instead of the brother file's filename itself.
	 * @return
	 * 		the matched brother files, or empty array if 
	 * 		there's no any.
	 * @author sunhe
	 * @date Mar 20, 2016
	 */
	public static File[] getBrotherFiles(final File file, final Pattern pattern) {
		return file.getParentFile().listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				if (pattern.matcher(name).matches()) {
					return true;
				}
				else {
					return false;
				}
			}
			
		});
	}
	
	/**
	 * A brother file is a file that 
	 * resides in the same directory.
	 * 
	 * @param file
	 * 		current file
	 * @param brotherFileName
	 * 		brother file's filename to be matched
	 * @return
	 * 		matched brother file, null if no one matches
	 * @author sunhe
	 * @date Mar 21, 2016
	 */
	public static File getBrotherFile(File file, final String brotherFileName) {
		File[] files = file.getParentFile().listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				if (name.toLowerCase().equals(brotherFileName.toLowerCase())) {
					return true;
				}
				else {
					return false;
				}
			}
			
		});
		if (files.length == 0) {
			return null;
		}
		else {
			return files[0];
		}
	}
	
	/**
	 * 
	 * @param file
	 * @param brotherFileName
	 * @return
	 * 		the brother file to be created
	 * @author sunhe
	 * @date Mar 20, 2016
	 */
	public static File createBrotherFile(File file, String brotherFileName) {
		return new File(file.getParent() + "/" + brotherFileName);
	}
	
}
