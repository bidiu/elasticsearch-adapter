package cn.com.deepdata.es_adapter.task;

import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;

import cn.com.deepdata.es_adapter.PipelineContext;
import cn.com.deepdata.es_adapter.UnsupportedJsonFormatException;
import cn.com.deepdata.es_adapter.listener.ResponseListener;

/**
 * Runnable task for inbound mode.
 * <p/>
 * TODO thread model, especially exception related, of listener
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class InboundTask extends BoundTask {
	
	public InboundTask(PipelineContext pipelineCtx) {
		super(pipelineCtx);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		try {
			Object data = null;
			IndexRequestBuilder indexRequestBuilder = null;
			IndexResponse indexResponse = null;
			
			while ((data = dataQueue.take()) != dataQueuePoisonObj) {
				try {
					// adapt data to JSON format
					data = adapterChain.fireAdapters(data);
					
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
				catch (UnsupportedJsonFormatException e) {
					e.printStackTrace();
					((ResponseListener<IndexResponse>) pipelineCtx.getResponseListener()).onFailure(e);
					if (settings.doesStopOnError()) {
						break;
					}
				}
				catch (ElasticsearchException e) {
					// TODO listener
					e.printStackTrace();
					if (settings.doesStopOnError()) {
						break;
					}
				}
			}
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		catch (RuntimeException e) {
			e.printStackTrace();
		}
		finally {
			// tell other threads to exit
			dataQueue.add(dataQueuePoisonObj);
		}
	}
	
}
