package cn.com.deepdata.es_adapter.listener;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;

import cn.com.deepdata.es_adapter.ExceptionEvent;

/**
 * 当前没有被使用.
 */
public class DefaultBulkResponseListener implements ResponseListener<BulkResponse> {

	@Override
	public void onResponse(BulkResponse response) {
		if (response.hasFailures()) {
			for (BulkItemResponse bulkItemResponse : response) {
				if (bulkItemResponse.isFailed()) {
					System.out.println("Failure message: " + bulkItemResponse.getFailureMessage());
				}
			}
		}
	}

	@Override
	public void onException(ExceptionEvent event) {
		event.getCause().printStackTrace();
	}

}
