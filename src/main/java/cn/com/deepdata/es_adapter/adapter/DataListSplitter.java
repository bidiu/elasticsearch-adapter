package cn.com.deepdata.es_adapter.adapter;

import java.util.List;

import cn.com.deepdata.es_adapter.SkipAdaptingException;

/**
 * Adapter that split the incoming data with list type to 
 * individual elements
 * <p/>
 * T is the element's type.
 */
public class DataListSplitter<T> extends AbstractAdapter implements QueueDataProvidingAdapter {
	
	protected void split(List<T> list, AdapterContext ctx) {
		for (T ele : list) {
			while (true) {
				try {
					putData(ele, ctx.getNextAdapterClazz());
					break;
				}
				catch (InterruptedException e) {
					// swallow
				}
			}
		}
	}
	
	@Override
	public Object inboundAdapt(Object data, AdapterContext ctx) throws Exception {
		@SuppressWarnings("unchecked")
		List<T> list = (List<T>) data;
		split(list, ctx);
		throw new SkipAdaptingException();
	}

	@Override
	public Object outboundAdapt(Object data, AdapterContext ctx) throws Exception {
		@SuppressWarnings("unchecked")
		List<T> list = (List<T>) data;
		split(list, ctx);
		throw new SkipAdaptingException();
	}
	
}
