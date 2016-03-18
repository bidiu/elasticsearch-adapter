package cn.com.deepdata.es_adapter.listener;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;

/**
 * Log Elasticsearch index response to somewhere.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class LogActionListener implements ActionListener<IndexResponse> {

	@Override
	public void onResponse(IndexResponse response) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFailure(Throwable e) {
		// TODO Auto-generated method stub
		
	}
	
}
