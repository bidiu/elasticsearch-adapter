package cn.com.deepdata.es_adapter.task;

import java.util.concurrent.BlockingQueue;

import org.elasticsearch.client.Client;

import cn.com.deepdata.es_adapter.PipelineContext;
import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * The abstract class of {@link InboundTask}.
 * <p/>
 * TODO outbound task
 */
public abstract class AbstractPipelineTask implements Runnable {
	
	protected PipelineContext pipelineCtx;
	
	protected PipelineSettings settings;
	
	protected Client client;
	
	protected BlockingQueue<DataWrapper> dataQueue;
	
	protected AdapterChain adapterChain;
	
	protected Object dataQueuePoisonObj;
	
	protected Object realDataQueuePoisonObj;
	
	protected String index;
	
	protected String type;
	
	protected int timeoutAfterClosing;
	
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
	
}
