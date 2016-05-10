package cn.com.deepdata.es_adapter.task;

import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;

import cn.com.deepdata.es_adapter.PipelineContext;
import cn.com.deepdata.es_adapter.UnsupportedJsonFormatException;
import cn.com.deepdata.es_adapter.common.ExceptionEvent;
import cn.com.deepdata.es_adapter.listener.ResponseListener;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * Runnable task for inbound mode.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class InboundTask extends AbstractPipelineTask {
	
	public InboundTask(PipelineContext pipelineCtx) {
		super(pipelineCtx);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		try {
			DataWrapper dataWrapper = null;
			Object data = null;
			IndexRequestBuilder indexRequestBuilder = null;
			IndexResponse indexResponse = null;
			
			// take data from queue
			dataWrapper = dataQueue.take();
			data = dataWrapper.getData();
			while (data != dataQueuePoisonObj) {
				try {
					// adapt data to JSON format
					dataWrapper = adapterChain.fireAdapters(dataWrapper);
					data = dataWrapper.getData();
					
					// build index request
					if (data instanceof Map<?, ?>) {
						Map<String, Object> source = (Map<String, Object>) data;
						indexRequestBuilder = client.prepareIndex(index, type).setSource(source);
					}
					else if (data instanceof String) {
						String source = (String) data;
						indexRequestBuilder = client.prepareIndex(index, type).setSource(source);
					}
					else if (data instanceof XContentBuilder) {
						XContentBuilder source = (XContentBuilder) data;
						indexRequestBuilder = client.prepareIndex(index, type).setSource(source);
					}
					else if (data instanceof byte[]) {
						byte[] source = (byte[]) data;
						indexRequestBuilder = client.prepareIndex(index, type).setSource(source);
					}
					else {
						// unsupported JSON format
						throw new UnsupportedJsonFormatException("Unsupported JSON format; it should be one of these: String, Map<String, Object>, XContentBuilder, and byte[].");
					}
					
					// fire index request
					indexResponse = indexRequestBuilder.execute().actionGet();
					((ResponseListener<IndexResponse>) pipelineCtx.getResponseListener()).onResponse(indexResponse);
				}
				catch (Exception e) {
					try {
						((ResponseListener<IndexResponse>) pipelineCtx.getResponseListener())
								.onFailure(new ExceptionEvent(e, dataWrapper));
					}
					catch (RuntimeException runtimeE) {
						runtimeE.printStackTrace();
					}
				}
				
				// take data from queue for next loop
				dataWrapper = dataQueue.take();
				data = dataWrapper.getData();
			}
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		finally {
			// tell other threads to exit
			dataQueue.add(new DataWrapper(dataQueuePoisonObj));
		}
	}
	
}
