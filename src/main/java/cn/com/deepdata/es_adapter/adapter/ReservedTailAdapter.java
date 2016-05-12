package cn.com.deepdata.es_adapter.adapter;

/**
 * @author sunhe
 * @date May 12, 2016
 */
final class ReservedTailAdapter extends AbstractAdapter {

	@Override
	public Object inboundAdapt(Object data, AdapterContext ctx) throws Exception {
		return data;
	}

	@Override
	public Object outboundAdapt(Object data, AdapterContext ctx) throws Exception {
		return data;
	}

}
