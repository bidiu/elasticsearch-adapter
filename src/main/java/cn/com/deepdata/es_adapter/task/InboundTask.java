package cn.com.deepdata.es_adapter.task;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;

import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.PipelineContext;
import cn.com.deepdata.es_adapter.SkipAdaptingException;
import cn.com.deepdata.es_adapter.UnsupportedJsonFormatException;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * Runnable task for inbound mode.
 */
public class InboundTask extends AbstractPipelineTask {
	
	private boolean hasPoisonObjReceived = false;
	
	public InboundTask(PipelineContext pipelineCtx) {
		super(pipelineCtx);
	}
	
	private DataWrapper takeDataWrapperFromQueue() throws InterruptedException {
		DataWrapper dataWrapper = null;
		
		if (hasPoisonObjReceived) {
			dataWrapper = dataQueue.poll(timeoutAfterClosing, TimeUnit.SECONDS);
		}
		else {
			dataWrapper = dataQueue.take();
			if (dataWrapper.getData() == dataQueuePoisonObj) {
				hasPoisonObjReceived = true;
				dataWrapper = dataQueue.poll(timeoutAfterClosing, TimeUnit.SECONDS);
			}
		}
		
		if (dataWrapper != null && dataWrapper.getData() == realDataQueuePoisonObj) {
			return null;
		}
		else {
			return dataWrapper;
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		try {
			DataWrapper dataWrapper = null;
			Object data = null;
			IndexRequestBuilder indexRequestBuilder = null;
			IndexResponse indexResponse = null;
			
			while ((dataWrapper = takeDataWrapperFromQueue()) != null) {
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
					fireResponseListener(indexResponse);
				}
				catch (SkipAdaptingException e) {
					// swallow
				}
				catch (Exception e) {
					try {
						fireResponseListener(new ExceptionEvent(e, dataWrapper));
					}
					catch (RuntimeException runtimeE) {
						runtimeE.printStackTrace();
					}
				}
			}
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		finally {
			// tell other threads to exit
			dataQueue.add(new DataWrapper(realDataQueuePoisonObj));
		}
	}
	
}
