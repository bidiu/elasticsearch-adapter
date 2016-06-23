package cn.com.deepdata.es_adapter.adapter;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy;

import cn.com.deepdata.es_adapter.SkipAdaptingException;

/**
 * A simple adapter that adapts Microsoft Excel 97/2000/XP/2003 work sheet file (.xls) 
 * to a list of {@link Map}s (every adapted map will be populated to the {@link AdapterChain} 
 * individually, so immediately subsequent adapter of this one should be capable of 
 * processing {@link Map}, not a list of maps).
 * <p/>
 * This adapter is simplified, not very versatile - only supporting 1-layer, 2-dimensional 
 * layout of data. More specifically, the data to adapt should be like a 2-dimensional table, 
 * without inner complicated enclosing structure in it. And you can extend this class to 
 * provide more functionalities or change some default behaviors.
 * <p/>
 * This adapter can accept either {@link File} or {@link FileInputStream} representing the work 
 * sheet file to adapt as an input.
 */
public class SimpleXlsFile2MapsAdapter extends AbstractAdapter implements QueueDataProvidingAdapter {
	
	protected final List<String> titleList;
	
	protected final int startRowNum;
	protected final int endRowNum;
	
	protected final List<Integer> sheetIndicesToAdapt;
	protected final List<String> sheetNamesToAdapt;
	
	protected final boolean shouldTrimStr;
	
	/**
	 * @param titleList
	 * 		titles applied to each cell of a row
	 */
	public SimpleXlsFile2MapsAdapter(List<String> titleList) {
		this(titleList, 0, Integer.MAX_VALUE, true);
	}
	
	/**
	 * @param titleList
	 * 		titles applied to each cell of a row
	 * @param startRowNum
	 * 		number of row, from which and above to adapt. 
	 * 		0-based, inclusive. 
	 */
	public SimpleXlsFile2MapsAdapter(List<String> titleList, int startRowNum) {
		this(titleList, startRowNum, Integer.MAX_VALUE, true);
	}
	
	/**
	 * @param titleList
	 * 		titles applied to each cell of a row
	 * @param startRowNum
	 * 		number of row, from which and above to adapt. 
	 * 		0-based, inclusive. 
	 * @param shouldTrimStr
	 * 		whether string value should be trimed
	 */
	public SimpleXlsFile2MapsAdapter(List<String> titleList, int startRowNum, boolean shouldTrimStr) {
		this(titleList, startRowNum, Integer.MAX_VALUE, shouldTrimStr);
	}
	
	/**
	 * @param titleList
	 * 		titles applied to each cell of a row
	 * @param startRowNum
	 * 		number of row, from which and above to adapt. 
	 * 		0-based, inclusive. 
	 * @param endRowNum
	 * 		number of row, the last row to adapt. 
	 * 		0-based, exclusive.
	 * @param shouldTrimStr
	 * 		whether string value should be trimed
	 */
	public SimpleXlsFile2MapsAdapter(List<String> titleList, int startRowNum, int endRowNum, boolean shouldTrimStr) {
		this(titleList, null, null, startRowNum, endRowNum, shouldTrimStr);
	}
	
	/**
	 * @param titleList
	 * 		titles applied to each cell of a row
	 * @param sheetIndicesToAdapt
	 * 		index of sheets to adapt
	 */
	public SimpleXlsFile2MapsAdapter(List<String> titleList, List<Integer> sheetIndicesToAdapt) {
		this(titleList, sheetIndicesToAdapt, 0, true);
	}
	
	/**
	 * @param titleList
	 * 		titles applied to each cell of a row
	 * @param sheetIndicesToAdapt
	 * 		index of sheets to adapt
	 * @param startRowNum
	 * 		number of row, from which and above to adapt. 
	 * 		0-based, inclusive. 
	 * @param shouldTrimStr
	 * 		whether string value should be trimed
	 */
	public SimpleXlsFile2MapsAdapter(List<String> titleList, List<Integer> sheetIndicesToAdapt, 
			int startRowNum, boolean shouldTrimStr) {
		this(titleList, sheetIndicesToAdapt, null, startRowNum, Integer.MAX_VALUE, shouldTrimStr);
	}
	
	/**
	 * @param titleList
	 * 		titles applied to each cell of a row
	 * @param sheetNamesToAdapt
	 * 		name of sheets to adapt
	 * @param shouldTrimStr
	 * 		whether string value should be trimed
	 * @param startRowNum
	 * 		number of row, from which and above to adapt. 
	 * 		0-based, inclusive. 
	 */
	public SimpleXlsFile2MapsAdapter(List<String> titleList, List<String> sheetNamesToAdapt, 
			boolean shouldTrimStr, int startRowNum) {
		this(titleList, null, sheetNamesToAdapt, startRowNum, Integer.MAX_VALUE, shouldTrimStr);
	}
	
	/**
	 * At least one of the parameters of 'sheetIndicesToAdapt' and 'sheetNamesToAdapt' 
	 * should be null, or only 'sheetIndicesToAdapt' is applied.
	 * 
	 * @param titleList
	 * 		titles applied to each cell of a row
	 * @param sheetIndicesToAdapt
	 * 		index of sheets to adapt
	 * @param sheetNamesToAdapt
	 * 		name of sheets to adapt
	 * @param startRowNum
	 * 		number of row, from which and above to adapt. 
	 * 		0-based, inclusive. 
	 * @param endRowNum
	 * 		number of row, the last row to adapt. 
	 * 		0-based, exclusive.
	 * @param shouldTrimStr
	 * 		whether string value should be trimed
	 */
	protected SimpleXlsFile2MapsAdapter(List<String> titleList, List<Integer> sheetIndicesToAdapt, 
			List<String> sheetNamesToAdapt, int startRowNum, int endRowNum, boolean shouldTrimStr) {
		this.titleList = titleList;
		this.sheetIndicesToAdapt = sheetIndicesToAdapt == null ? 
				null : Collections.unmodifiableList(sheetIndicesToAdapt);
		this.sheetNamesToAdapt = sheetNamesToAdapt == null ? 
				null : Collections.unmodifiableList(sheetNamesToAdapt);
		this.startRowNum = startRowNum;
		this.endRowNum = endRowNum;
		this.shouldTrimStr = shouldTrimStr;
	}
	
	/*
	 * You can override the following extract* methods to 
	 * change the default behavior how to extract value from 
	 * different types of cell.
	 */
	protected Object extractValueFromStringTypeCell(HSSFCell cell) {
		return cell.getStringCellValue();
	}
	protected Object extractValueFromNumericTypeCell(HSSFCell cell) {
		return cell.getNumericCellValue();
	}
	protected Object extractValueFromBooleanTypeCell(HSSFCell cell) {
		return cell.getBooleanCellValue();
	}
	protected Object extractValueFromBlankCell(HSSFCell cell) {
		return null;
	}
	protected Object extractValueFromOtherTypeCell(HSSFCell cell) {
		throw new IllegalStateException("cell type not supported");
	}
	
	protected void adaptRow(HSSFRow row, AdapterContext ctx) throws InterruptedException {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		
		for (int c = 0; c < titleList.size(); c++) {
			HSSFCell cell = row.getCell(c, MissingCellPolicy.CREATE_NULL_AS_BLANK);
			String title = titleList.get(c);
			
			int cellType = cell.getCellType();
			if (cellType == HSSFCell.CELL_TYPE_STRING) {
				dataMap.put(title, extractValueFromStringTypeCell(cell));
			}
			else if (cellType == HSSFCell.CELL_TYPE_NUMERIC) {
				dataMap.put(title, extractValueFromNumericTypeCell(cell));
			}
			else if (cellType == HSSFCell.CELL_TYPE_BOOLEAN) {
				dataMap.put(title, extractValueFromBooleanTypeCell(cell));
			}
			else if (cellType == HSSFCell.CELL_TYPE_BLANK) {
				dataMap.put(title, extractValueFromBlankCell(cell));
			}
			else {
				dataMap.put(title, extractValueFromOtherTypeCell(cell));
			}
		}
		
		putData(dataMap, ctx.getNextAdapterClazz());
	}
	
	protected void adaptSheet(HSSFSheet sheet, AdapterContext ctx) throws InterruptedException {
		for (int r = startRowNum; r < sheet.getLastRowNum() + 1 && r < endRowNum; r++) {
			HSSFRow row = sheet.getRow(r);
			if (row != null) {
				adaptRow(row, ctx);
			}
		}
	}
	
	@Override
	public Object inboundAdapt(Object data, AdapterContext ctx) throws Exception {
		FileInputStream in = null;
		HSSFWorkbook workbook = null;
		
		try {
			in = data instanceof File ? new FileInputStream((File) data) : (FileInputStream) data;
			workbook = new HSSFWorkbook(in);
			
			List<HSSFSheet> sheetsToAdapt = new ArrayList<HSSFSheet>();
			if (sheetIndicesToAdapt != null) {
				for (int sheetIndex : sheetIndicesToAdapt) {
					sheetsToAdapt.add(workbook.getSheetAt(sheetIndex));
				}
			}
			else if (sheetNamesToAdapt != null) {
				for (String sheetName : sheetNamesToAdapt) {
					sheetsToAdapt.add(workbook.getSheet(sheetName));
				}
			}
			else  {
				for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
					sheetsToAdapt.add(workbook.getSheetAt(i));
				}
			}
			
			for (HSSFSheet sheet : sheetsToAdapt) {
				adaptSheet(sheet, ctx);
			}
			
			throw new SkipAdaptingException();
		}
		finally {
			if (in != null) {
				in.close();
			}
			if (workbook != null) {
				workbook.close();
			}
		}
	}

	@Override
	public Object outboundAdapt(Object data, AdapterContext ctx) throws Exception {
		throw new IllegalStateException("This adapter cannot be used in outbound mode");
	}
	
}
