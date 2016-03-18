package cn.com.deepdata.es_adapter.adapter;

/**
 * Implementation of this class should be thread-safe.
 * <p/>
 * And note that you'd better only override the abstract methods 
 * of this class.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public abstract class AbstractAdapter implements Adapter {
	
	@Override
	public Object inboundAdapt(AdapterContext ctx, Object data) {
		return ctx.fireNextAdapter(inboundAdapt(data));
	}
	
	public abstract Object inboundAdapt(Object data);
	
	@Override
	public Object outboundAdapt(AdapterContext ctx, Object data) {
		return ctx.fireNextAdapter(outboundAdapt(data));
	}
	
	public abstract Object outboundAdapt(Object data);
	
}
