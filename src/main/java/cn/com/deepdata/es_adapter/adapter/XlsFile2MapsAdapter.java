package cn.com.deepdata.es_adapter.adapter;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import cn.com.deepdata.es_adapter.SkipAdaptingException;

public class XlsFile2MapsAdapter extends AbstractAdapter implements QueueDataProvidingAdapter {
	
	protected final List<String> titleList;
	
	protected final int startRowNum;
	
	protected final int endRowNum;
	
	public XlsFile2MapsAdapter(List<String> titleList) {
		
	}
	
	public XlsFile2MapsAdapter(List<String> titleList, int startRowNum) {
		this(titleList, startRowNum, Integer.MAX_VALUE);
	}
	
	public XlsFile2MapsAdapter(List<String> titleList, int startRowNum, int endRowNum) {
		this.titleList = Collections.unmodifiableList(titleList);
		this.startRowNum = startRowNum;
		this.endRowNum = endRowNum;
	}
	
	public XlsFile2MapsAdapter(List<String> titleList, List<Integer> sheetIndeciesToAdapt) {
		
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
