package cn.com.deepdata.es_adapter.listener;

import org.elasticsearch.action.index.IndexResponse;

import cn.com.deepdata.es_adapter.common.ExceptionEvent;

/**
 * Default index response listener that will log the response somewhere.
 * <p/>
 * This class is thread-safe.
 * <p/>
 * TODO somewhere should be the current directory
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class DefaultIndexResponseListener implements ResponseListener<IndexResponse> {

	@Override
	public void onResponse(IndexResponse response) {
		// do nothing
	}

	@Override
	public void onFailure(ExceptionEvent event) {
		event.getCause().printStackTrace();
	}
	
}
