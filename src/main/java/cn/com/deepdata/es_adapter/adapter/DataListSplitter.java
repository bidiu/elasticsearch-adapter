package cn.com.deepdata.es_adapter.adapter;

import java.util.List;

import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.SkipAdaptingException;
import cn.com.deepdata.es_adapter.model.DataWrapper;

public class DataListSplitter<T> extends AbstractAdapter {
	
	protected void split(List<T> list, AdapterContext ctx) {
		for (T ele : list) {
			try {
				getDataQueue().put(new DataWrapper(ele, ctx.getNextAdapterClazz()));
			}
			catch (InterruptedException e) {
				// swallow
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
	
	@Override
	public Object onInboundException(ExceptionEvent event) {
		event.setShouldPropagate(false);
		return event;
	}
	
	@Override
	public Object onOutboundException(ExceptionEvent event) {
		event.setShouldPropagate(false);
		return event;
	}
	
}
