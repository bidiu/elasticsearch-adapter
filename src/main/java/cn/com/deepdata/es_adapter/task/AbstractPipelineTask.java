package cn.com.deepdata.es_adapter.task;

import java.util.concurrent.BlockingQueue;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import cn.com.deepdata.es_adapter.PipelineContext;
import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.listener.ResponseListener;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * The abstract class of {@link InboundTask}.
 * <p/>
 * TODO outbound task
 */
public abstract class AbstractPipelineTask implements Runnable {
	
	protected final PipelineContext pipelineCtx;
	
	protected final PipelineSettings settings;
	
	protected final Client client;
	
	protected final BlockingQueue<DataWrapper> dataQueue;
	
	protected final AdapterChain adapterChain;
	
	protected final Object dataQueuePoisonObj;
	
	protected final Object realDataQueuePoisonObj;
	
	protected final String index;
	
	protected final String type;
	
	protected final int timeoutAfterClosing;
	
	public AbstractPipelineTask(PipelineContext pipelineCtx) {
		this.pipelineCtx = pipelineCtx;
		settings = pipelineCtx.getSettings();
		client = pipelineCtx.getClient();
		dataQueue = pipelineCtx.getDataQueue();
		adapterChain = pipelineCtx.getAdapterChain();
		dataQueuePoisonObj = pipelineCtx.getDataQueuePoisonObj();
		realDataQueuePoisonObj = pipelineCtx.getRealDataQueuePoisonObj();
		index = settings.getIndex();
		type = settings.getType();
		timeoutAfterClosing = settings.getTimeoutAfterClosing();
	}
	
	@SuppressWarnings("unchecked")
	protected void fireResponseListener(ActionResponse response) {
		if (settings.isInbound()) {
			if (settings.isBulk()) {
				((ResponseListener<BulkResponse>) pipelineCtx.getResponseListener())
						.onResponse((BulkResponse) response);
			}
			else {
				((ResponseListener<IndexResponse>) pipelineCtx.getResponseListener())
						.onResponse((IndexResponse) response);
			}
		}
		else {
			// TODO
		}
	}
	
	protected void fireResponseListener(ExceptionEvent event) {
		pipelineCtx.getResponseListener().onException(event);
	}
	
}
