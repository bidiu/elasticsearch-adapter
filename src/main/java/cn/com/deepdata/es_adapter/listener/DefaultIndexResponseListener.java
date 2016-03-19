package cn.com.deepdata.es_adapter.listener;

import org.elasticsearch.action.index.IndexResponse;

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
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFailure(Throwable e) {
		// TODO Auto-generated method stub
		
	}
	
}
