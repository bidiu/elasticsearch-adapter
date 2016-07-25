package cn.com.deepdata.es_adapter;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.NodeBuilder;

import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.common.Closeable;
import cn.com.deepdata.es_adapter.example.CopyrightTrans;
import cn.com.deepdata.es_adapter.listener.DefaultIndexResponseListener;
import cn.com.deepdata.es_adapter.listener.ResponseListener;
import cn.com.deepdata.es_adapter.model.DataWrapper;
import cn.com.deepdata.es_adapter.task.InboundTask;

/**
 * {@link Pipeline}是数据源到Elasticsearch集群的抽象，可以使用它集成不同的组件（配置，
 * 适配器，监听器）以完成向Elasticsearch集群灌入数据的目的。
 * <p/>
 * <strong>注意</strong>，因为导入操作是异步的，使用后必须调用{@link #close()}
 * 同步阻塞并关闭pipeline，否则会产生数据丢失，不能入库.
 * <p/>
 * This class is thread-safe.
 * <p/>
 * 如何实例化和使用本类，see {@link CopyrightTrans}.
 * <p/>
 * 
 * <b>Features not yet supported</b>:<br>
 * TODO bulk mode <br/>
 * TODO outbound mode <br/>
 * TODO log4j <br/>
 */
public class Pipeline implements Closeable {
	
	/**
	 * This class is {@link Pipeline} settings.
	 * <p/>
	 * 不可变类，实例化后可以重复使用.
	 */
	public static class PipelineSettings {
		
		// settings name
		public static final String IS_INBOUND = "isInbound";
		public static final String DATA_QUEUE_CAPACITY = "dataQueueCapacity";
		public static final String THREAD_POOL_SIZE = "threadPoolSize";
		public static final String CLUSTER_NAME = "clusterName";
		public static final String HOST = "host";
		public static final String PORT = "port";
		public static final String INDEX = "index";
		public static final String TYPE = "type";
		public static final String IS_BULK = "isBulk";
		public static final String TIMEOUT_AFTER_CLOSING = "timeoutAfterClosing";
		
		// default value
		public static final boolean DEFAULT_IS_INBOUND = true;
		public static final int DEFAULT_DATA_QUEUE_CAPACITY = 4096;
		public static final int DEFAULT_THREAD_POOL_SIZE = 8;
		public static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
		public static final int DEFAULT_PORT = 9300;
		public static final boolean DEFAULT_IS_BULK = false;
		/** In second unit */
		public static final int DEFAULT_TIMEOUT_AFTER_CLOSING = 0;
		
		/**
		 * Builder that can build {@link PipelineSettings}.
		 * <p/>
		 * 大部分参数有合理的默认值.
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
				map.put(PORT, String.valueOf(DEFAULT_PORT));
				map.put(IS_BULK, String.valueOf(DEFAULT_IS_BULK));
				map.put(TIMEOUT_AFTER_CLOSING, String.valueOf(DEFAULT_TIMEOUT_AFTER_CLOSING));
			}
			
			/*
			 * setters ..
			 */
			public SettingsBuilder inbound() {
				map.put(IS_INBOUND, String.valueOf(true));
				return this;
			}
			public SettingsBuilder outbound() {
				throw new IllegalStateException("Outbound mode currently is not supported.");
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
			public SettingsBuilder bulk(boolean isBulk) {
				map.put(IS_BULK, String.valueOf(isBulk));
				return this;
			}
			/**
			 * @param timeoutAfterClosing
			 * 		In second unit.
			 * @return
			 */
			public SettingsBuilder timeoutAfterClosing(int timeoutAfterClosing) {
				map.put(TIMEOUT_AFTER_CLOSING, String.valueOf(timeoutAfterClosing));
				return this;
			}
			
			/**
			 * Build a {@link PipelineSettings}.
			 * 
			 * @return
			 */
			public PipelineSettings build() {
				PipelineSettings settings =  new PipelineSettings();
				
				// set all of the settings
				settings.setInbound(Boolean.valueOf(map.get(IS_INBOUND)));
				settings.setDataQueueCapacity(Integer.parseInt(map.get(DATA_QUEUE_CAPACITY)));
				settings.setThreadPoolSize(Integer.parseInt(map.get(THREAD_POOL_SIZE)));
				settings.setClusterName(map.get(CLUSTER_NAME));
				settings.setHost(map.get(HOST));
				settings.setPort(Integer.parseInt(map.get(PORT)));
				settings.setIndex(map.get(INDEX));
				settings.setType(map.get(TYPE));
				settings.setBulk(Boolean.valueOf(map.get(IS_BULK)));
				settings.setTimeoutAfterClosing(Integer.parseInt(map.get(TIMEOUT_AFTER_CLOSING)));
				return settings;
			}
			
		}
		
		/**
		 * inboud模式代表向Elasticsearch集群导入数据，outbound模式反之（当前不支持）.
		 * <p/>
		 * Inbound mode by default.
		 */
		private boolean isInbound;
		
		/**
		 * All data will be staged into a queue for a while, this attribute is 
		 * the queue's size - the number of data unit, typically document.
		 * <p/>
		 * 4096 by default.
		 */
		private int dataQueueCapacity;
		
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
		 * false by default. (bulk模式当前不支持).
		 */
		private boolean isBulk;
		
		/**
		 * In second unit.
		 */
		private int timeoutAfterClosing;
		
		private PipelineSettings() {
			
		}
		
		/*
		 * getters ..
		 */
		public boolean isInbound() {
			return isInbound;
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
		public int getTimeoutAfterClosing() {
			return timeoutAfterClosing;
		}
		
		/*
		 * setters ..
		 */
		private synchronized void setInbound(boolean isInbound) {
			this.isInbound = isInbound;
		}
		private synchronized void setDataQueueCapacity(int dataQueueCapacity) {
			this.dataQueueCapacity = dataQueueCapacity;
		}
		private synchronized void setThreadPoolSize(int threadPoolSize) {
			this.threadPoolSize = threadPoolSize;
		}
		private synchronized void setClusterName(String clusterName) {
			this.clusterName = clusterName;
		}
		private synchronized void setHost(String host) {
			this.host = host;
		}
		private synchronized void setPort(int port) {
			this.port = port;
		}
		private synchronized void setIndex(String index) {
			this.index = index;
		}
		private synchronized void setType(String type) {
			this.type = type;
		}
		private synchronized void setBulk(boolean isBulk) {
			this.isBulk = isBulk;
		}
		private synchronized void setTimeoutAfterClosing(int timeoutAfterClosing) {
			this.timeoutAfterClosing = timeoutAfterClosing;
		}
		
		public static SettingsBuilder builder() {
			return new SettingsBuilder();
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
		 */
		public boolean validate() {
			if (index == null || type == null) {
				return false;
			}
			else {
				return true;
			}
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
	public synchronized Client getClient() {
		return pipelineCtx.getClient();
	}
	public synchronized PipelineSettings getPipelineSettings() {
		return pipelineCtx.getSettings();
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
	 * Build the pipeline.
	 * 
	 * @param settings
	 * @param adapterChainInitializer
	 * @param indexResponseListener
	 * @return
	 */
	public static Pipeline build(
			PipelineSettings settings, 
			AdapterChainInitializer adapterChainInitializer, 
			ResponseListener<? extends ActionResponse> responseListener) {
		if (! settings.validate()) {
			throw new IllegalArgumentException("Invalid settings, cannot build pipeline.");
		}
		
		Pipeline pipeline = new Pipeline();
		
		// pipeline context related ..
		BlockingQueue<DataWrapper> dataQueue = new LinkedBlockingQueue<DataWrapper>(settings.getDataQueueCapacity());
		AdapterChain adapterChain = new AdapterChain(settings.isInbound(), dataQueue);
		if (adapterChainInitializer != null) {
			adapterChainInitializer.initialize(adapterChain);
		}
		Client client = null;
		if (settings.getHost() == null) {
			client = NodeBuilder.nodeBuilder()
			        .settings(ImmutableSettings.settingsBuilder().put("http.enabled", false))
			        .client(true)
			        .clusterName(settings.getClusterName())
			        .node()
			        .client();
		}
		else {
			client = new TransportClient(ImmutableSettings.settingsBuilder()
						.put("cluster.name", settings.getClusterName())
						.put("client.transport.sniff", true)
						.build())
					.addTransportAddress(new InetSocketTransportAddress(settings.getHost(), settings.getPort()));
		}
		PipelineContext pipelineCtx = new PipelineContext(settings, client, 
				dataQueue, adapterChain, pipeline, responseListener);
		pipeline.setPipelineCtx(pipelineCtx);
		
		// thread pool related ..
		pipeline.setExecutorService(Executors.newFixedThreadPool(settings.getThreadPoolSize()));
		for (int i = 0; i < settings.getThreadPoolSize(); i++) {
			if (settings.isInbound()) {
				pipeline.getExecutorService().submit(new InboundTask(pipelineCtx));
			}
			else {
				// TODO outbound mode
			}
		}
		return pipeline;
	}
	
	/**
	 * Build the pipeline.
	 * 
	 * @param settings
	 * @param adapterChainInitializer
	 * @return
	 */
	public static Pipeline build(
			PipelineSettings settings, 
			AdapterChainInitializer adapterChainInitializer) {
		return build(settings, adapterChainInitializer, new DefaultIndexResponseListener());
	}
	
	/**
	 * Build the pipeline.
	 * 
	 * @param settings
	 * @param responseListener
	 * @return
	 */
	public static Pipeline build(
			PipelineSettings settings, 
			ResponseListener<? extends ActionResponse> responseListener) {
		return build(settings, null, responseListener);
	}

	/**
	 * Build the pipeline.
	 * 
	 * @param settings
	 * @return
	 */
	public static Pipeline build(PipelineSettings settings) {
		return build(settings, null, new DefaultIndexResponseListener());
	}
	
	/**
	 * 向数据队列中放入数据，数据将会被适配并导入到Elasticsearch集群.
	 * 
	 * @param data
	 * @throws InterruptedException
	 */
	public void putData(Object data) throws InterruptedException {
		if (isClosed) {
			throw new IllegalStateException("Pipeline already been closed.");
		}
		
		if (pipelineCtx.getSettings().isInbound()) {
			pipelineCtx.getDataQueue().put(new DataWrapper(data));
		}
		else {
			throw new IllegalStateException("Not allowed to take data from queue when in inbound mode.");
		}
	}
	
	/**
	 * 当前不支持outbound mode，所以不需要调用这个方法.
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public Object takeData() throws InterruptedException {
		if (isClosed) {
			throw new IllegalStateException("Pipeline already been closed.");
		}
		
		if (pipelineCtx.getSettings().isInbound()) {
			throw new IllegalStateException("Not allowed to put data to queue when in outbound mode.");
		}
		else {
			return pipelineCtx.getDataQueue().take().getData();
		}
	}
	
	/**
	 * 同步阻塞等待数据队列被清空，并关闭pipeline. 关闭之后，不能再向数据队列中添加数据，否则会抛出
	 * 异常.
	 * <p/>
	 * Also see {@link Pipeline}.
	 * <p/>
	 * Also see {@link cn.com.deepdata.es_adapter.common.Closeable#close()}.
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
				pipelineCtx.getDataQueue().put(new DataWrapper(pipelineCtx.getDataQueuePoisonObj()));
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
		
		// the only data in the queue should be real poison pill
		if (pipelineCtx.getDataQueue().size() > 1) {
			throw new IllegalStateException(
					"The queue is still being populated with some data when trying to close the pipeline, " + 
					"basically this is because you use some adapters that also put data into the queue. " + 
					"Except for this, other data has been transfered successfully. " + 
					"If you want to retry in order to tranfer the data still being in the queue, " + 
					"first try to undo the data that has been transfered, " + 
					"and then set the configuration parameter 'timeoutAfterClosing' with a proper value (default is 0 second). " + 
					"For more information about this issue, please refer JavaDoc of QueueDataProvidingAdapter.");
		}
	}
	
}
