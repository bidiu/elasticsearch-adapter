package cn.com.deepdata.es_adapter;

import java.util.concurrent.LinkedBlockingQueue;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;

/**
 * TODO support delete ..
 * TODO log
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class Pipeline {
	
	/**
	 * Pipeline settings class
	 * <p/>
	 * This class is thread-safe.
	 * 
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public static class PipelineSettings {
		
		public static final boolean DEFAULT_IS_INBOUND = true;
		public static final int DEFAULT_DATA_QUEUE_CAPACITY = 4096;
		public static final int DEFAULT_THREAD_POOL_SIZE = 4;
		public static final boolean DEFAULT_IS_BULK = false;
		
		/** inbound means put data into elasticsearch */
		private boolean isInbound;
		
		/** Elasticsearch node to be used to generate a client */
		private Node node;
		
		private int dataQueueCapacity;
		
		private AdapterChainInitializer adapterChainInitializer;
		
		private int threadPoolSize;
		
		private String index;
		
		private String type;
		
		private boolean isBulk;
		
		private PipelineSettings() {
			// dummy..
		}
		
		public synchronized boolean isInbound() {
			return isInbound;
		}

		public synchronized Node getNode() {
			return node;
		}

		public synchronized int getDataQueueCapacity() {
			return dataQueueCapacity;
		}
		
		public synchronized int getThreadPoolSize() {
			return threadPoolSize;
		}
		
		public synchronized String getIndex() {
			return index;
		}
		
		public synchronized String getType() {
			return type;
		}
		
		public synchronized boolean isBulk() {
			return isBulk;
		}
		
		public synchronized AdapterChainInitializer getAdapterChainInitializer() {
			return adapterChainInitializer;
		}

		public synchronized PipelineSettings inbound() {
			isInbound = true;
			return this;
		}
		
		public synchronized PipelineSettings outbound() {
			isInbound = false;
			return this;
		}
		
		public synchronized PipelineSettings node(Node node) {
			this.node = node;
			return this;
		}
		
		public synchronized PipelineSettings dataQueueCapacity(int dataQueueCapacity) {
			this.dataQueueCapacity = dataQueueCapacity;
			return this;
		}
		
		public synchronized PipelineSettings adapterChainInitializer(AdapterChainInitializer adapterChainInitializer) {
			this.adapterChainInitializer = adapterChainInitializer;
			return this;
		}
		
		public synchronized PipelineSettings threadPoolSize(int threadPoolSize) {
			this.threadPoolSize = threadPoolSize;
			return this;
		}
		
		public synchronized PipelineSettings index(String index) {
			this.index = index;
			return this;
		}
		
		public synchronized PipelineSettings type(String type) {
			this.type = type;
			return this;
		}
		
		public synchronized PipelineSettings bulk(boolean isBulk) {
			this.isBulk = isBulk;
			return this;
		}
		
		public boolean validate() {
			if (index == null || type == null) {
				return false;
			}
			else {
				return true;
			}
		}

		/**
		 * Get pipeline's default settings.
		 * 
		 * @return
		 * @author sunhe
		 * @date 2016年3月18日
		 */
		public static PipelineSettings getDefaultSettings(String clusterName) {
			PipelineSettings settings =  new PipelineSettings();
			
			// set default settings..
			Node node = NodeBuilder.nodeBuilder()
				        .settings(ImmutableSettings.settingsBuilder().put("http.enabled", false))
				        .client(true)
				        .clusterName(clusterName)
				        .node();
			return settings.inbound()
					.node(node)
					.dataQueueCapacity(DEFAULT_DATA_QUEUE_CAPACITY)
					.threadPoolSize(DEFAULT_THREAD_POOL_SIZE)
					.bulk(DEFAULT_IS_BULK);
		}
		
		/**
		 * Get pipeline's default settings with cluster name 'elasticsearch'.
		 * 
		 * @return
		 * @author sunhe
		 * @date 2016年3月18日
		 */
		public static PipelineSettings getDefaultSettings() {
			return getDefaultSettings("elasticsearch");
		}
		
	}
	
	private PipelineContext pipelineCtx;
	
	private PipelineContext getPipelineCtx() {
		return pipelineCtx;
	}
	
	private void setPipelineCtx(PipelineContext pipelineCtx) {
		this.pipelineCtx = pipelineCtx;
	}
	
	private Pipeline() {
		// dummy..
	}

	/**
	 * Note that after invoking this methods, the parameter settings
	 * should be discarded, which means the settings MUST NOT be altered.
	 * 
	 * 
	 * @param settings
	 * @return
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public static Pipeline build(PipelineSettings settings) {
		if (! settings.validate()) {
			throw new IllegalArgumentException("Invalid settings, cannot build pipeline.");
		}
		Pipeline pipeline = new Pipeline();
		
		// set pipeline's component..
		AdapterChain adapterChain = new AdapterChain(settings.isInbound());
		if (settings.getAdapterChainInitializer() != null) {
			settings.getAdapterChainInitializer().initialize(adapterChain);
		}
		PipelineContext pipelineCtx = new PipelineContext(settings, settings.getNode().client(), 
				new LinkedBlockingQueue<Object>(settings.getDataQueueCapacity()), adapterChain, 
				pipeline);
		pipeline.setPipelineCtx(pipelineCtx);
		// TODO Runnable related
		
		return pipeline;
	}
	
	/**
	 * Call this method when you are in inbound mode.
	 * 
	 * @param data
	 * @throws InterruptedException
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public void putData(Object data) throws InterruptedException {
		if (pipelineCtx.getSettings().isInbound) {
			pipelineCtx.getDataQueue().put(data);
		}
		else {
			throw new IllegalStateException("Not allowed to put data into queue when in inbound mode.");
		}
	}
	
	/**
	 * Call this method when you are in outbound mode.
	 * 
	 * @return
	 * @throws InterruptedException
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public Object takeData() throws InterruptedException {
		if (pipelineCtx.getSettings().isInbound) {
			throw new IllegalStateException("Not allowed to take data from queue when in outbound mode.");
		}
		else {
			return pipelineCtx.getDataQueue().take();
		}
	}
	
}
