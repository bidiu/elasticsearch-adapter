package cn.com.deepdata.es_adapter.listener;

import org.elasticsearch.action.ActionListener;

/**
 * This interface only extends the interface {@link org.elasticsearch.action.ActionListener}.
 * <p/>
 * Users can implement this interface to perform some logic that have something 
 * to do with the incoming Elasticsearch response, logging error responses somewhere, 
 * for example.
 * <p/>
 * If the user don't assign any user-specific listener, the corresponding default 
 * listener will be used, for instance, {@link DefaultIndexResponseListener}.
 * <p/>
 * Note that implementation of this interface MUST be thread-safe.
 * 
 * @author sunhe
 * @date Mar 19, 2016
 */
public interface ResponseListener<Response> extends ActionListener<Response> {

}