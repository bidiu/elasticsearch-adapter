package cn.com.deepdata.es_adapter;

import java.util.concurrent.BlockingQueue;

import org.elasticsearch.client.Client;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;

/**
 * Immutable class
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class PipelineContext {
	
	private PipelineSettings settings;
	
	private Client client;
	
	private BlockingQueue<Object> dataQueue;
	
	private AdapterChain adapterChain;
	
	private Pipeline pipeline;
	
	private Object dataQueuePoisonObj;
	
	public PipelineContext(PipelineSettings settings, Client client, 
			BlockingQueue<Object> dataQueue, AdapterChain adapterChain, 
			Pipeline pipeline) {
		this.settings = settings;
		this.client = client;
		this.dataQueue = dataQueue;
		this.adapterChain = adapterChain;
		this.pipeline = pipeline;
		dataQueuePoisonObj = new Object();
	}

	public synchronized PipelineSettings getSettings() {
		return settings;
	}

	public synchronized Client getClient() {
		return client;
	}

	public synchronized BlockingQueue<Object> getDataQueue() {
		return dataQueue;
	}

	public synchronized AdapterChain getAdapterChain() {
		return adapterChain;
	}

	public synchronized Pipeline getPipeline() {
		return pipeline;
	}

	public synchronized Object getDataQueuePoisonObj() {
		return dataQueuePoisonObj;
	}
	
}
