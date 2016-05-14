package cn.com.deepdata.es_adapter;

public class UnsupportedJsonFormatException extends RuntimeException {

	private static final long serialVersionUID = -4240085860426410703L;
	
	public UnsupportedJsonFormatException() {
		// empty
	}
	
	public UnsupportedJsonFormatException(String msg) {
		super(msg);
	}

}
