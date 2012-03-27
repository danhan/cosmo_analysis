package cos.dataset.query.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface CosmoProtocol extends CoprocessorProtocol{

	// for both of them, because their is what   
	public  ArrayList<String>  propertyFilter(String family, String proper_name,
			String compareOp,int dataType, String threshold,Scan scan) throws IOException;

	// for both of them, because their is what   
	public List  getSpecificParticle(String family, String proper_name,
			String compareOp,int dataType, String threshold,Scan scan) throws IOException;	
	  // 
	ArrayList<String> getUniqueCoprocs4S1(long s1, long s2,Scan scan) throws IOException;
	  
	HashMap<String,String> getUniqueCoprocs4S2(String s1, String s2,Scan scan) throws IOException;
	  
	HashMap<String, HashMap<Long, String>>  changeTrendCop4S1(Scan scan) throws IOException;  
	
	HashMap<String, HashMap<Long, String>>  changeTrendCop4S2(Scan scan) throws IOException;
	
	// for test for no return from server
	// for both of them, because their is what   
	public void  test4PropertyFilter(String family, String proper_name,
			String compareOp,int dataType, String threshold,Scan scan) throws IOException;
	
	  
}
