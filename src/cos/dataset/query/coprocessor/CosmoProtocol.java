package cos.dataset.query.coprocessor;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface CosmoProtocol extends CoprocessorProtocol{

	// for both of them, because their is what   
	Map<String, String> propertyFilter(String[] result_families, String[] result_columns, Scan scan) throws IOException;
	  
	  // 
	Collection<String> getUniqueCoprocs4S1(int type, long s1, long s2,Scan scan) throws IOException;
	  
	Collection<String> getUniqueCoprocs4S2(int type, long s1, long s2,Scan scan) throws IOException;
	  
	  
	  
}
