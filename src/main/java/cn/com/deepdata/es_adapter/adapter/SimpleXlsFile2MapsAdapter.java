package cn.com.deepdata.es_adapter.adapter;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import cn.com.deepdata.es_adapter.SkipAdaptingException;

public class XlsFile2MapsAdapter extends AbstractAdapter implements QueueDataProvidingAdapter {
	
	protected final List<String> titleList;
	
	protected final int startRowNum;
	
	protected final int endRowNum;
	
	protected final List<Integer> sheetIndeciesToAdapt;
	
	protected final List<String> sheetNamesToAdapt;
	
	public XlsFile2MapsAdapter(List<String> titleList) {
		this(titleList, 0, Integer.MAX_VALUE);
	}
	
	public XlsFile2MapsAdapter(List<String> titleList, int startRowNum) {
		this(titleList, startRowNum, Integer.MAX_VALUE);
	}
	
	public XlsFile2MapsAdapter(List<String> titleList, int startRowNum, int endRowNum) {
		this(titleList, null, null, startRowNum, endRowNum);
	}
	
	public XlsFile2MapsAdapter(List<String> titleList, List<Integer> sheetIndeciesToAdapt) {
		this(titleList, sheetIndeciesToAdapt, 0);
	}
	
	public XlsFile2MapsAdapter(List<String> titleList, List<Integer> sheetIndeciesToAdapt, int startRowNum) {
		this(titleList, sheetIndeciesToAdapt, null, startRowNum, Integer.MAX_VALUE);
	}
	
	public XlsFile2MapsAdapter(List<String> titleList, int startRowNum, List<String> sheetNamesToAdapt) {
		this(titleList, null, sheetNamesToAdapt, startRowNum, Integer.MAX_VALUE);
	}
	
	private XlsFile2MapsAdapter(List<String> titleList, List<Integer> sheetIndeciesToAdapt, List<String> sheetNamesToAdapt, 
			int startRowNum, int endRowNum) {
		this.titleList = titleList;
		this.sheetIndeciesToAdapt = Collections.unmodifiableList(sheetIndeciesToAdapt);
		this.sheetNamesToAdapt = Collections.unmodifiableList(sheetNamesToAdapt);
		this.startRowNum = startRowNum;
		this.endRowNum = endRowNum;
	}
	
	protected void adaptRow(HSSFRow row) {
		// TODO
	}
	
	protected void adaptSheet(HSSFSheet sheet) {
		// TODO
	}
	
	@Override
	public Object inboundAdapt(Object data, AdapterContext ctx) throws Exception {
		FileInputStream in = null;
		
		try {
			in = data instanceof File ? new FileInputStream((File) data) : (FileInputStream) data;
			HSSFWorkbook workbook = new HSSFWorkbook(in);
			
			throw new SkipAdaptingException();
		}
		finally {
			if (in != null) {
				in.close();
			}
		}
	}

	@Override
	public Object outboundAdapt(Object data, AdapterContext ctx) throws Exception {
		throw new IllegalStateException("This adapter cannot be used in outbound mode");
	}
	
}
