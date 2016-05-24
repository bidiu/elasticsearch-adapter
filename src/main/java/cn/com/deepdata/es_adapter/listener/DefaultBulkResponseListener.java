package cn.com.deepdata.es_adapter.listener;

import org.elasticsearch.action.bulk.BulkResponse;

import cn.com.deepdata.es_adapter.ExceptionEvent;

public class DefaultBulkResponseListener implements ResponseListener<BulkResponse> {

	@Override
	public void onResponse(BulkResponse response) {
		// TODO
	}

	@Override
	public void onException(ExceptionEvent event) {
		event.getCause().printStackTrace();
	}

}
