package cn.com.deepdata.es_adapter.adapter;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * 适配器链有若干适配器{@link Adapter}和元数据组成.
 * <p/>
 * This class is thread-safe.
 */
public class AdapterChain {
	
	private boolean isInbound;
	
	private Adapter firstAdapter;
	
	private List<AdapterContext> adapterCtxList;
	
	private BlockingQueue<DataWrapper> dataQueue;

	public AdapterChain(boolean isInbound, BlockingQueue<DataWrapper> dataQueue) {
		this.isInbound = isInbound;
		firstAdapter = null;
		adapterCtxList = new LinkedList<AdapterContext>();
		this.dataQueue = dataQueue;
	}
	
	/**
	 * This is an utility function.
	 * <p/>
	 * Note that this function is only capable of setting the declared fields, 
	 * as opposed to the inherited fields.
	 */
	private static void setPrivateFieldByReflection(Object obj, String fieldName, Object value) throws ReflectiveOperationException {
		Class<?> clazz = obj.getClass();
		Field field = clazz.getDeclaredField(fieldName);
		field.setAccessible(true);
		field.set(obj, value);
	}
	
	public synchronized boolean isInbound() {
		return isInbound;
	}
	
	/**
	 * 向适配器链尾部添加一个适配器.
	 * 
	 * @param adapter
	 * 		the adapter going to add
	 * @return
	 * 		the adapter chain itself
	 */
	public synchronized AdapterChain addLast(Adapter adapter) {
		return addLast(adapter, new HashMap<String, Object>());
	}
	
	/**
	 * 向适配器链尾部添加一个适配器和它的自定义消息.
	 * 
	 * @param adapter
	 * 		the adapter going to add
	 * @param msg
	 * 		the custom message associated with the given adapter
	 * @return
	 * 		the adapter chain itself
	 */
	public synchronized AdapterChain addLast(Adapter adapter, Map<String, Object> msg) {
		try {
			// prepare adapter's dataQueue property
			if (adapter instanceof AbstractAdapter) {
				Class<?> clazz = adapter.getClass();
				while (clazz != AbstractAdapter.class) {
					clazz = clazz.getSuperclass();
				}
				Field field = clazz.getDeclaredField("dataQueue");
				field.setAccessible(true);
				field.set(adapter, dataQueue);
			}
			
			AdapterContext adapterCtx = new AdapterContext();
			setPrivateFieldByReflection(adapterCtx, "msg", msg);
			
			if (adapterCtxList.size() == 0) {
				firstAdapter = adapter;
			}
			else if (adapterCtxList.size() > 0) {
				// link current rear adapter context to current added one
				AdapterContext lastAdapterCtx = adapterCtxList.get(adapterCtxList.size() - 1);
				setPrivateFieldByReflection(lastAdapterCtx, "nextAdapterCtx", adapterCtx);
				setPrivateFieldByReflection(lastAdapterCtx, "nextAdapter", adapter);
			}
			
			setPrivateFieldByReflection(adapterCtx, "adapterChain", this);
			setPrivateFieldByReflection(adapterCtx, "nextAdapterCtx", null);
			setPrivateFieldByReflection(adapterCtx, "nextAdapter", null);
			adapterCtxList.add(adapterCtx);
			return this;
		}
		catch (ReflectiveOperationException e) {
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * 传入要适配的数据，数据从适配器链的第一个适配器到最后一个依次适配.
	 * 
	 * @param dataWrapper
	 * 		data to be processed
	 * @return
	 * 		适配后的数据
	 * @throws Exception 
	 */
	public DataWrapper fireAdapters(DataWrapper dataWrapper) throws Exception {
		if (firstAdapter == null) {
			// not any adapter available on the chain
			return dataWrapper;
		}
		else if (isInbound) {
			return firstAdapter.inboundAdapt(adapterCtxList.get(0), dataWrapper);
		}
		else {
			return firstAdapter.outboundAdapt(adapterCtxList.get(0), dataWrapper);
		}
	}
	
}
