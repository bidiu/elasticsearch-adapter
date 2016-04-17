package cn.com.deepdata.es_adapter;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction.NewClusterStateListener;
import org.junit.Ignore;
import org.junit.Test;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.AbstractAdapter;
import cn.com.deepdata.es_adapter.adapter.Adapter;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.adapter.AdapterContext;
import cn.com.deepdata.es_adapter.common.ExceptionEvent;
import cn.com.deepdata.es_adapter.listener.ResponseListener;

@SuppressWarnings("unused")
public class CommonTest {

	@Test
	public void test() throws InterruptedException {
		
	}

}
