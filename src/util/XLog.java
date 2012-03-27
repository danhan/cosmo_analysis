package util;

public class XLog {
	
	public static String DEBUG = "DEBUG";
	public static String STAT = "STAT";

	public void debug(String msg){
		System.out.println(DEBUG+": "+msg);
	}
	
	/*
	 * msg is a json format
	 * a=>xx;b=>yy
	 */
	public void statistic(String msg){
		System.out.println(STAT+": "+"{"+msg+"}");
	}	
	
	
	
}
