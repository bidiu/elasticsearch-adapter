package cn.com.deepdata.es_adapter;

/**
 * Users can throw this exception in their custom adapters
 * to quit the normal adapting flow, or say, there's no output (adapted) 
 * data to be further processed. And the exception will be swallowed by 
 * the library, which is desirable.
 */
public class SkipAdaptingException extends Exception {

	private static final long serialVersionUID = 5736957710381378729L;
	
	public SkipAdaptingException() {
		// empty
	}
	
	public SkipAdaptingException(String msg) {
		super(msg);
	}

}
