package cn.com.deepdata.es_adapter;

/**
 * 
 * @author sunhe
 * @date 2016年5月10日
 */
public class SkipAdaptingException extends RuntimeException {

	private static final long serialVersionUID = 5736957710381378729L;
	
	public SkipAdaptingException() {
		// empty
	}
	
	public SkipAdaptingException(String msg) {
		super(msg);
	}

}
