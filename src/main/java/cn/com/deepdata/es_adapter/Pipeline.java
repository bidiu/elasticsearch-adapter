package cn.com.deepdata.es_adapter;

import java.util.concurrent.LinkedBlockingQueue;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.common.Closeable;

/**
 * {@link Pipeline} is an abstract of channel that connect a data source to an 
 * Elasticsearch instance, so that data can be transmitted into or out of the 
 * instance. Client can use this class to aggregate different components of the 
 * library.
 * <p/>
 * Note that this class MUST be closed by invoking the method {@link #close()} 
 * when not used any more.
 * <p/>
 * 
 * TODO support delete .. <br/>
 * TODO log <br/>
 * TODO break point <br/>
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class Pipeline implements Closeable {
	
	/**
	 * This class is {@link Pipeline} settings.
	 * <p/>
	 * And this class is thread-safe.
	 * 
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public static class PipelineSettings {
		
		public static final boolean DEFAULT_IS_INBOUND = true;
		public static final int DEFAULT_DATA_QUEUE_CAPACITY = 4096;
		public static final int DEFAULT_THREAD_POOL_SIZE = 4;
		public static final boolean DEFAULT_IS_BULK = false;
		
		/**
		 * Inbound mode means that data are transmitted into Elasticsearch, while
		 * outbound mode means that data are transmitted out of Elasticsearch.
		 * <p/>
		 * Inbound mode by default.
		 */
		private boolean isInbound;
		
		/**
		 * Elasticsearch node to be used to generate a client.
		 */
		private Node node;
		
		/**
		 * All data will be staged into a queue for a while, this attribute is 
		 * the queue's size - the number of data unit, typically document.
		 * <p/>
		 * 4096 by default.
		 */
		private int dataQueueCapacity;
		
		private AdapterChainInitializer adapterChainInitializer;
		
		/**
		 * 4 by default.
		 */
		private int threadPoolSize;
		
		private String index;
		
		private String type;
		
		/**
		 * Whether the operation with Elasticsearch is in bulk mode, 
		 * false by default. 
		 */
		private boolean isBulk;
		
		private PipelineSettings() {
			// dummy..
		}
		
		/*
		 * Getters ..
		 */
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
		
		/**
		 * Set inbound mode.
		 * 
		 * @return
		 * @author sunhe
		 * @date Mar 18, 2016
		 */
		public synchronized PipelineSettings inbound() {
			isInbound = true;
			return this;
		}
		
		/**
		 * Set outbound mode.
		 * 
		 * @return
		 * @author sunhe
		 * @date Mar 18, 2016
		 */
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
		
		/**
		 * Validate whether the settings itself is okay.
		 * 
		 * @return
		 * @author sunhe
		 * @date Mar 18, 2016
		 */
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
		 * @param clusterName
		 * 		Elasticsearch cluster name
		 * @return
		 * @author sunhe
		 * @date Mar 18, 2016
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
		 * Get pipeline's default settings with cluster name "elasticsearch".
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
	
	private synchronized PipelineContext getPipelineCtx() {
		return pipelineCtx;
	}
	
	private synchronized void setPipelineCtx(PipelineContext pipelineCtx) {
		this.pipelineCtx = pipelineCtx;
	}
	
	private Pipeline() {
		// dummy..
	}

	/**
	 * Build a pipeline with a given settings.
	 * <p/>
	 * Note that after invoking this methods, the parameter "settings"
	 * MUST be discarded from outside, which means the settings MUST 
	 * NOT be altered any more, otherwise the outcome is undefined.
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
		
		// set pipeline's component ..
		AdapterChain adapterChain = new AdapterChain(settings.isInbound());
		if (settings.getAdapterChainInitializer() != null) {
			settings.getAdapterChainInitializer().initialize(adapterChain);
		}
		PipelineContext pipelineCtx = new PipelineContext(settings, settings.getNode().client(), 
				new LinkedBlockingQueue<Object>(settings.getDataQueueCapacity()), adapterChain, 
				pipeline);
		pipeline.setPipelineCtx(pipelineCtx);
		// TODO Runnable related ..
		
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
			throw new IllegalStateException("Not allowed to take data from queue when in inbound mode.");
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
			throw new IllegalStateException("Not allowed to put data to queue when in outbound mode.");
		}
		else {
			return pipelineCtx.getDataQueue().take();
		}
	}
	
	@Override
	public void close() {
		// TODO
	}
	
}
