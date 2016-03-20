package cn.com.deepdata.es_adapter;

/**
 * 
 * @author sunhe
 * @date Mar 20, 2016
 */
public class UnsupportedJsonFormatException extends RuntimeException {

	private static final long serialVersionUID = -4240085860426410703L;
	
	public UnsupportedJsonFormatException() {
		
	}
	
	public UnsupportedJsonFormatException(String msg) {
		super(msg);
	}

}
