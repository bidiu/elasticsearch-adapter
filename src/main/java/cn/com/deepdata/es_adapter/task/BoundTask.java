package cn.com.deepdata.es_adapter.task;

import java.util.concurrent.BlockingQueue;

import org.elasticsearch.client.Client;

import cn.com.deepdata.es_adapter.PipelineContext;
import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;

/**
 * The abstract class of {@link InboundTask} and {@link OutboundTask}.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public abstract class BoundTask implements Runnable {
	
	protected PipelineContext pipelineCtx;
	
	protected PipelineSettings settings;
	
	protected Client client;
	
	protected BlockingQueue<Object> dataQueue;
	
	protected AdapterChain adapterChain;
	
	protected Object dataQueuePoisonObj;
	
	protected String index;
	
	protected String type;
	
	public BoundTask(PipelineContext pipelineCtx) {
		this.pipelineCtx = pipelineCtx;
		settings = pipelineCtx.getSettings();
		client = pipelineCtx.getClient();
		dataQueue = pipelineCtx.getDataQueue();
		adapterChain = pipelineCtx.getAdapterChain();
		dataQueuePoisonObj = pipelineCtx.getDataQueuePoisonObj();
		index = settings.getIndex();
		type = settings.getType();
	}
	
}
