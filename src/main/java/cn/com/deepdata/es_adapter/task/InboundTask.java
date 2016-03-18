package cn.com.deepdata.es_adapter.task;

import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;

import cn.com.deepdata.es_adapter.PipelineContext;

/**
 * Runnable task in inbound mode
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
			
			while ((data = dataQueue.take()) != dataQueuePoisonObj) {
				// adapt the incoming data to JSON format
				data = adapterChain.fireFirstAdapter(data);
				if (data instanceof Map<?, ?>) {
					Map<String, Object> source = (Map<String, Object>) data;
					// TODO
					client.prepareIndex(index, type).setSource(source).execute();
				}
				else if (data instanceof String) {
					String source = (String) data;
					// TODO
					client.prepareIndex(index, type).setSource(source);
				}
				else if (data instanceof XContentBuilder) {
					XContentBuilder source = (XContentBuilder) data;
					// TODO
					client.prepareIndex(index, type).setSource(source);
				}
				else if (data instanceof byte[]) {
					byte[] source = (byte[]) data;
					// TODO
					client.prepareIndex(index, type).setSource(source);
				}
				else {
					// TODO
				}
			}
			// tell other threads to exit
			dataQueue.add(dataQueuePoisonObj);
		}
		catch (RuntimeException e) {
			// TODO
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			// TODO
			e.printStackTrace();
		}
	}
	
}
