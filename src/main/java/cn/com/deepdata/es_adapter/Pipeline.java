package cn.com.deepdata.es_adapter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.common.Closeable;
import cn.com.deepdata.es_adapter.listener.DefaultIndexResponseListener;
import cn.com.deepdata.es_adapter.listener.ResponseListener;
import cn.com.deepdata.es_adapter.task.InboundTask;

/**
 * {@link Pipeline} is an abstract of channel that connect a data source to an 
 * Elasticsearch cluster, so that data can be transmitted into or out of the 
 * cluster. Client can use this class to aggregate different components of the 
 * library and perform the main operations against Elasticsearch.
 * <p/>
 * Note that this class MUST be closed by invoking the method {@link #close()} 
 * when not used any more.
 * <p/>
 * This class is thread-safe.
 * <p/>
 * 
 * TODO log <br/>
 * TODO break point <br/>
 * TODO more advanced listener <br/>
 * TODO refine Javadoc
 * TODO support PipelineSettings deep copy
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class Pipeline implements Closeable {
	
	/**
	 * This class is {@link Pipeline} settings.
	 * <p/>
	 * Note that different {@link Pipeline} MUST NOT share the same 
	 * {@link PipelineSettings} instance.
	 * <p/>
	 * And this class is thread-safe.
	 * 
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public static class PipelineSettings {
		
		public static final boolean DEFAULT_IS_INBOUND = true;
		public static final int DEFAULT_DATA_QUEUE_CAPACITY = 4096;
		public static final int DEFAULT_THREAD_POOL_SIZE = 8;
		public static final boolean DEFAULT_IS_BULK = false;
		public static final boolean DEFAULT_DOES_STOP_ON_ERROR = false;
		
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
		 * 8 by default.
		 */
		private int threadPoolSize;
		
		private String index;
		
		private String type;
		
		/**
		 * Whether the operation with Elasticsearch is in bulk mode, 
		 * false by default. 
		 */
		private boolean isBulk;
		
		private boolean doesStopOnError;
		
		private ResponseListener<IndexResponse> indexResponseListener;
		
		private PipelineSettings() {
			
		}
		
		/*
		 * getters ..
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
		public synchronized ResponseListener<IndexResponse> getIndexResponseListener() {
			return indexResponseListener;
		}
		public synchronized boolean doesStopOnError() {
			return doesStopOnError;
		}
		
		/*
		 * setters ..
		 */
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
		public synchronized PipelineSettings stopOnError(boolean doesStopOnError) {
			this.doesStopOnError = doesStopOnError;
			return this;
		}
		/**
		 * Note that bulk mode currently is not supported.
		 * <p/>
		 * TODO
		 * 
		 * @param isBulk
		 * @return
		 * @author sunhe
		 * @date Mar 20, 2016
		 */
		public synchronized PipelineSettings bulk(boolean isBulk) {
			this.isBulk = isBulk;
			return this;
		}
		public synchronized PipelineSettings indexResponseListener(ResponseListener<IndexResponse> indexResponseListener) {
			this.indexResponseListener = indexResponseListener;
			return this;
		}
		
		/**
		 * Validate whether the settings itself is okay.
		 * <p/>
		 * Note that this method only determine whether user has 
		 * assign must-set attributes of the settings, which means that 
		 * the settings may not be valid even though this method returns 
		 * true.
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
					.bulk(DEFAULT_IS_BULK)
					.indexResponseListener(new DefaultIndexResponseListener())
					.stopOnError(DEFAULT_DOES_STOP_ON_ERROR);
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
	
	private boolean isClosed;
	
	private ExecutorService executorService;
	
	private Pipeline() {
		
	}
	
	/*
	 * getters and setters ..
	 */
	public synchronized boolean isClosed() {
		return isClosed;
	}
	private synchronized ExecutorService getExecutorService() {
		return executorService;
	}
	private synchronized void setPipelineCtx(PipelineContext pipelineCtx) {
		this.pipelineCtx = pipelineCtx;
	}
	private synchronized void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

	/**
	 * Build a pipeline with a given settings.
	 * <p/>
	 * Note that after invoking this methods, the parameter "settings"
	 * MUST be discarded from outside, which means the settings MUST 
	 * NOT be altered any more from outside, otherwise the outcome is 
	 * undefined.
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
		// pipeline context related ..
		AdapterChain adapterChain = new AdapterChain(settings.isInbound());
		if (settings.getAdapterChainInitializer() != null) {
			settings.getAdapterChainInitializer().initialize(adapterChain);
		}
		PipelineContext pipelineCtx = new PipelineContext(settings, settings.getNode().client(), 
				new LinkedBlockingQueue<Object>(settings.getDataQueueCapacity()), adapterChain, 
				pipeline);
		pipeline.setPipelineCtx(pipelineCtx);
		// thread pool related ..
		pipeline.setExecutorService(Executors.newFixedThreadPool(settings.getThreadPoolSize()));
		for (int i = 0; i < settings.getThreadPoolSize(); i++) {
			if (settings.isInbound()) {
				pipeline.getExecutorService().submit(new InboundTask(pipelineCtx));
			}
			else {
				// TODO
			}
		}
		return pipeline;
	}
	
	/**
	 * Put data into the managed blocking queue, which then will be transmitted 
	 * into Elasticsearch cluster. If the queue is full right now, the method will 
	 * block until there is space available in the queue or the current thread is 
	 * interrupted, either comes first.
	 * <p/>
	 * Call this method when you are in inbound mode.
	 * <p/>
	 * Note that there is one and only one unit of data for each thread 
	 * that probably will still be put into queue right after the pipeline 
	 * is closed by another thread. For most of the time, you do not have to
	 * worry about this.
	 * 
	 * @param data
	 * @throws InterruptedException
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public void putData(Object data) throws InterruptedException {
		if (isClosed) {
			throw new IllegalStateException("Pipeline already been closed.");
		}
		
		if (pipelineCtx.getSettings().isInbound()) {
			pipelineCtx.getDataQueue().put(data);
		}
		else {
			throw new IllegalStateException("Not allowed to take data from queue when in inbound mode.");
		}
	}
	
	/**
	 * Take Elasticsearch-hosted data from the managed blocking queue. If the queue 
	 * is empty right now, the method will block until there is data available in the 
	 * queue or the current thread is interrupted, either comes first.
	 * <p/>
	 * Call this method when you are in outbound mode.
	 * <p/>
	 * Note that there is one and only one unit of data for each thread 
	 * that probably will still be taken from queue right after the pipeline 
	 * is closed by another thread. For most of the time, you do not have to
	 * worry about this.
	 * <p/>
	 * TODO poison pill related
	 * 
	 * @return
	 * @throws InterruptedException
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public Object takeData() throws InterruptedException {
		if (isClosed) {
			throw new IllegalStateException("Pipeline already been closed.");
		}
		
		if (pipelineCtx.getSettings().isInbound()) {
			throw new IllegalStateException("Not allowed to put data to queue when in outbound mode.");
		}
		else {
			return pipelineCtx.getDataQueue().take();
		}
	}
	
	/**
	 * Close the pipeline. After the pipeline is closed, attempts that put data into 
	 * or retrieve from queue will cause exception. More specifically, you should not 
	 * invoke the methods {@link cn.com.deepdata.es_adapter.Pipeline#putData(Object) putData(Object)} 
	 * and {@link cn.com.deepdata.es_adapter.Pipeline#takeData() takeData()} after the pipeline
	 * is closed, otherwise exception will occur.
	 * <p/>
	 * Also see {@link cn.com.deepdata.es_adapter.common.Closeable#close()}.
	 * 
	 * @author sunhe
	 * @date Mar 19, 2016
	 */
	@Override
	public synchronized void close() {
		if (isClosed) {
			// already closed
			return;
		}
		
		isClosed = true;
		boolean isInterrupted = false;
		while (! executorService.isTerminated()) {
			try {
				pipelineCtx.getDataQueue().put(pipelineCtx.getDataQueuePoisonObj());
				executorService.shutdown();
				executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e) {
				isInterrupted = true;
				// retry to close, since the close operation cannot be interrupted
			}
		}
		pipelineCtx.getClient().close();
		if (isInterrupted) {
			// tell the caller current thread was interrupted during close operation
			Thread.currentThread().interrupt();
		}
	}
	
}
