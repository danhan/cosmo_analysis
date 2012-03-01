package cos.dataset.query.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface CosmoProtocol extends CoprocessorProtocol{

	// for both of them, because their is what   
	public HashMap<String, HashMap<String, String>>  propertyFilter(String family, String proper_name,
			String compareOp,int dataType, String threshold,Scan scan) throws IOException;
	  
	  // 
	ArrayList<String> getUniqueCoprocs4S1(long s1, long s2,Scan scan) throws IOException;
	  
	ArrayList<String> getUniqueCoprocs4S2(long s1, long s2,Scan scan) throws IOException;
	  
	  
	  
}
