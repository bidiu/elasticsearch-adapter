package cn.com.deepdata.es_adapter.adapter;

/**
 * 这是一个内部使用的适配器.
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
