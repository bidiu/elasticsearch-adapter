package cn.com.deepdata.es_adapter.listener;

import org.elasticsearch.action.index.IndexResponse;

import cn.com.deepdata.es_adapter.ExceptionEvent;

/**
 * Default index response listener that will log the response somewhere.
 * <p/>
 * This class is thread-safe.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class DefaultIndexResponseListener implements ResponseListener<IndexResponse> {

	@Override
	public void onResponse(IndexResponse response) {
		// empty
	}

	@Override
	public void onException(ExceptionEvent event) {
		event.getCause().printStackTrace();
	}
	
}
