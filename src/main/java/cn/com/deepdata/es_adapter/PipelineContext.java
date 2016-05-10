package cn.com.deepdata.es_adapter;

import java.util.concurrent.BlockingQueue;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.listener.ResponseListener;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * This class is {@link Pipeline pipeline} context.
 * <p/>
 * And this class is a immutable one.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class PipelineContext {
	
	private PipelineSettings settings;
	
	private Client client;
	
	private BlockingQueue<DataWrapper> dataQueue;
	
	private AdapterChain adapterChain;
	
	private Pipeline pipeline;
	
	private ResponseListener<? extends ActionResponse> responseListener;
	
	private Object dataQueuePoisonObj;
	
	private Object realDataQueuePoisonObj;
	
	public PipelineContext(PipelineSettings settings, Client client, 
			BlockingQueue<DataWrapper> dataQueue, AdapterChain adapterChain, 
			Pipeline pipeline, ResponseListener<? extends ActionResponse> responseListener) {
		this.settings = settings;
		this.client = client;
		this.dataQueue = dataQueue;
		this.adapterChain = adapterChain;
		this.pipeline = pipeline;
		this.responseListener = responseListener;
		dataQueuePoisonObj = new Object();
		realDataQueuePoisonObj = new Object();
	}
	
	/*
	 * getters ..
	 */
	public PipelineSettings getSettings() {
		return settings;
	}
	public Client getClient() {
		return client;
	}
	public BlockingQueue<DataWrapper> getDataQueue() {
		return dataQueue;
	}
	public AdapterChain getAdapterChain() {
		return adapterChain;
	}
	public Pipeline getPipeline() {
		return pipeline;
	}
	public Object getDataQueuePoisonObj() {
		return dataQueuePoisonObj;
	}
	public Object getRealDataQueuePoisonObj() {
		return realDataQueuePoisonObj;
	}
	public ResponseListener<? extends ActionResponse> getResponseListener() {
		return responseListener;
	}
	
}
