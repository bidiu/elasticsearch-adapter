package cn.com.deepdata.es_adapter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.common.Closeable;
import cn.com.deepdata.es_adapter.listener.DefaultIndexResponseListener;
import cn.com.deepdata.es_adapter.listener.ResponseListener;
import cn.com.deepdata.es_adapter.task.InboundTask;

/**
 * {@link Pipeline} is an abstract of channel that connects a data source to an 
 * Elasticsearch cluster, so that data can be transmitted into or out of the 
 * cluster. Client can use this class to aggregate different components of the 
 * elasticsearch-adapter library and perform operations against Elasticsearch.
 * <p/>
 * Note that this class MUST be closed by invoking the method {@link #close()} 
 * when not used any more.
 * <p/>
 * This class is thread-safe.
 * <p/>
 * 
 * TODO support log4j <br/>
 * TODO break point <br/>
 * TODO more advanced listener <br/>
 * TODO refine Javadoc <br/>
 * TODO support PipelineSettings deep copy <br/>
 * TODO settings have a bug <br/>
 * TODO aggregation/delimiter apdater 
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class Pipeline implements Closeable {
	
	/**
	 * This class is {@link Pipeline} settings.
	 * <p/>
	 * The instance of this class is immutable and can be used repeatedly.
	 * 
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public static class PipelineSettings {
		
		// name
		public static final String IS_INBOUND = "isInbound";
		public static final String DATA_QUEUE_CAPACITY = "dataQueueCapacity";
		public static final String THREAD_POOL_SIZE = "threadPoolSize";
		public static final String CLUSTER_NAME = "clusterName";
		public static final String HOST = "host";
		public static final String PORT = "port";
		public static final String INDEX = "index";
		public static final String TYPE = "type";
		public static final String IS_BULK = "isBulk";
		public static final String DOES_STOP_ON_ERROR = "doesStopOnError";
		
		// default value
		public static final boolean DEFAULT_IS_INBOUND = true;
		public static final int DEFAULT_DATA_QUEUE_CAPACITY = 4096;
		public static final int DEFAULT_THREAD_POOL_SIZE = 8;
		public static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
		public static final boolean DEFAULT_IS_BULK = false;
		public static final boolean DEFAULT_DOES_STOP_ON_ERROR = false;
		
		/**
		 * Builder that can build {@link PipelineSettings}.
		 * 
		 * @author sunhe
		 * @date 2016年4月15日
		 */
		public static class SettingsBuilder {
			
			private Map<String, String> map;
			
			private SettingsBuilder() {
				map = new ConcurrentHashMap<String, String>();
	
				// here set the default value
				map.put(IS_INBOUND, String.valueOf(DEFAULT_IS_INBOUND));
				map.put(DATA_QUEUE_CAPACITY, String.valueOf(DEFAULT_DATA_QUEUE_CAPACITY));
				map.put(THREAD_POOL_SIZE, String.valueOf(DEFAULT_THREAD_POOL_SIZE));
				map.put(CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
				map.put(IS_BULK, String.valueOf(DEFAULT_IS_BULK));
				map.put(DOES_STOP_ON_ERROR, String.valueOf(DEFAULT_DOES_STOP_ON_ERROR));
			}
			
			/*
			 * setters ..
			 */
			public SettingsBuilder inbound() {
				map.put(IS_INBOUND, String.valueOf(true));
				return this;
			}
			public SettingsBuilder outbound() {
				map.put(IS_INBOUND, String.valueOf(false));
				return this;
			}
			public SettingsBuilder dataQueueCapacity(int dataQueueCapacity) {
				map.put(DATA_QUEUE_CAPACITY, String.valueOf(dataQueueCapacity));
				return this;
			}
			public SettingsBuilder threadPoolSize(int threadPoolSize) {
				map.put(THREAD_POOL_SIZE, String.valueOf(threadPoolSize));
				return this;
			}
			public SettingsBuilder clusterName(String clusterName) {
				map.put(CLUSTER_NAME, clusterName);
				return this;
			}
			public SettingsBuilder host(String host) {
				map.put(HOST, host);
				return this;
			}
			public SettingsBuilder port(int port) {
				map.put(PORT, String.valueOf(port));
				return this;
			}
			public SettingsBuilder index(String index) {
				map.put(INDEX, index);
				return this;
			}
			public SettingsBuilder type(String type) {
				map.put(TYPE, type);
				return this;
			}
			public SettingsBuilder stopOnError(boolean doesStopOnError) {
				map.put(DOES_STOP_ON_ERROR, String.valueOf(doesStopOnError));
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
			public SettingsBuilder bulk(boolean isBulk) {
				map.put(IS_BULK, String.valueOf(isBulk));
				return this;
			}
			
			/**
			 * Build a {@link PipelineSettings}.
			 * 
			 * @return
			 * @author sunhe
			 * @date 2016年4月15日
			 */
			public PipelineSettings build() {
				PipelineSettings settings =  new PipelineSettings();
				
				// set all of the settings
				settings.setInbound(Boolean.valueOf(map.get(IS_INBOUND)));
				settings.dataQueueCapacity(Integer.parseInt(map.get(DATA_QUEUE_CAPACITY)));
				settings.threadPoolSize(Integer.parseInt(map.get(THREAD_POOL_SIZE)));
				// TODO cluster name
				settings.host(map.get(HOST));
				settings.port(Integer.parseInt(map.get(PORT)));
				settings.index(map.get(INDEX));
				settings.type(map.get(TYPE));
				settings.bulk(Boolean.valueOf(map.get(IS_BULK)));
				settings.stopOnError(Boolean.valueOf(map.get(DOES_STOP_ON_ERROR)));
				return settings;
			}
			
		}
		
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
		
		private String clusterName;
		
		/**
		 * the host of one of the Elasticsearch instance in the cluster
		 */
		private String host;
		
		/**
		 * the port of one of the Elasticsearch instance in the cluster
		 */
		private int port;
		
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
		public boolean isInbound() {
			return isInbound;
		}
		public Node getNode() {
			return node;
		}
		public int getDataQueueCapacity() {
			return dataQueueCapacity;
		}
		public int getThreadPoolSize() {
			return threadPoolSize;
		}
		public String getClusterName() {
			return clusterName;
		}
		public String getHost() {
			return host;
		}
		public int getPort() {
			return port;
		}
		public String getIndex() {
			return index;
		}
		public String getType() {
			return type;
		}
		public boolean isBulk() {
			return isBulk;
		}
		public AdapterChainInitializer getAdapterChainInitializer() {
			return adapterChainInitializer;
		}
		public ResponseListener<IndexResponse> getIndexResponseListener() {
			return indexResponseListener;
		}
		public boolean doesStopOnError() {
			return doesStopOnError;
		}
		
		/*
		 * setters ..
		 */
		private synchronized void setInbound(boolean isInbound) {
			this.isInbound = isInbound;
		}
		private synchronized PipelineSettings node(Node node) {
			this.node = node;
			return this;
		}
		private synchronized void dataQueueCapacity(int dataQueueCapacity) {
			this.dataQueueCapacity = dataQueueCapacity;
		}
		private synchronized PipelineSettings adapterChainInitializer(AdapterChainInitializer adapterChainInitializer) {
			this.adapterChainInitializer = adapterChainInitializer;
			return this;
		}
		private synchronized void threadPoolSize(int threadPoolSize) {
			this.threadPoolSize = threadPoolSize;
		}
		private synchronized void setClusterName(String clusterName) {
			this.clusterName = clusterName;
		}
		private synchronized void host(String host) {
			this.host = host;
		}
		private synchronized void port(int port) {
			this.port = port;
		}
		private synchronized void index(String index) {
			this.index = index;
		}
		private synchronized void type(String type) {
			this.type = type;
		}
		private synchronized void stopOnError(boolean doesStopOnError) {
			this.doesStopOnError = doesStopOnError;
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
		private synchronized void bulk(boolean isBulk) {
			this.isBulk = isBulk;
		}
		private synchronized PipelineSettings indexResponseListener(ResponseListener<IndexResponse> indexResponseListener) {
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
			settings.clusterName = clusterName;
			
			// set default settings..
			Node node = null;
			if (settings.getHost() == null) {
				node = NodeBuilder.nodeBuilder()
					        .settings(ImmutableSettings.settingsBuilder().put("http.enabled", false))
					        .client(true)
					        .clusterName(clusterName)
					        .node();
			}
			return settings.setInbound()
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
			return getDefaultSettings(DEFAULT_CLUSTER_NAME);
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
		Client client = null;
		if (settings.getHost() == null) {
			client = settings.getNode().client();
		}
		else {
			client = new TransportClient(ImmutableSettings.settingsBuilder()
							.put("cluster.name", settings.getClusterName())
							.put("client.transport.sniff", true)
							.build())
					.addTransportAddress(new InetSocketTransportAddress(settings.getHost(), settings.getPort()));
		}
		PipelineContext pipelineCtx = new PipelineContext(settings, client, 
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
