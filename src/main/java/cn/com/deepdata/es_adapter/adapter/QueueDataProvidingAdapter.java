package cn.com.deepdata.es_adapter.adapter;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings.SettingsBuilder;

/**
 * Using of adapters of this type indicates that you'd better 
 * assign the 'timoutAfterClosing' parameter by invoking 
 * {@link SettingsBuilder#timeoutAfterClosing(int)}.
 * <p/>
 * 这是由与采用的线程模型导致的，否则会引起小部分数据丢失，未能入库.
 */
public interface QueueDataProvidingAdapter {
	
	// empty
	
}
