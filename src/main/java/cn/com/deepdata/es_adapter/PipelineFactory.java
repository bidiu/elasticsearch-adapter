package cn.com.deepdata.es_adapter;

import org.elasticsearch.action.ActionResponse;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.listener.ResponseListener;

/**
 * Sometimes, if you want to build multiple pipelines from 
 * same configurations, then you can use this class.
 * <p/>
 * This is class is immutable.
 */
public class PipelineFactory {
	
	private final PipelineSettings settings;
	
	private final AdapterChainInitializer adapterChainInitializer;
	
	private final ResponseListener<? extends ActionResponse> responseListener;
	
	public PipelineFactory(PipelineSettings settings, 
			AdapterChainInitializer adapterChainInitializer, 
			ResponseListener<? extends ActionResponse> responseListener) {
		this.settings = settings;
		this.adapterChainInitializer = adapterChainInitializer;
		this.responseListener = responseListener;
	}
	
	public PipelineSettings getSettings() {
		return settings;
	}

	public AdapterChainInitializer getAdapterChainInitializer() {
		return adapterChainInitializer;
	}

	public ResponseListener<? extends ActionResponse> getResponseListener() {
		return responseListener;
	}

	public Pipeline build() {
		return Pipeline.build(settings, adapterChainInitializer, responseListener);
	}
	
}
