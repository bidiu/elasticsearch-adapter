package cn.com.deepdata.es_adapter;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Ignore;
import org.junit.Test;

public class ClientTest {

	@SuppressWarnings("resource")
	@Ignore
	@Test
	public void test() {
		Client client = new TransportClient(ImmutableSettings.settingsBuilder()
				.put("cluster.name", "elasticsearch")
				.put("client.transport.sniff", true)
				.build())
		.addTransportAddress(new InetSocketTransportAddress("221.122.121.96", 19300));
		
		client.prepareIndex("copyright", "software_copyright").setSource("{\"message\":\"hello, world\"}").execute().actionGet();
		
		client.close();
	}

}
