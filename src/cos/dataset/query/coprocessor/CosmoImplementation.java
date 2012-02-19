package cos.dataset.query.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import util.Common;

public class CosmoImplementation extends BaseEndpointCoprocessor implements
		CosmoProtocol {

	static final Log log = LogFactory.getLog(CosmoImplementation.class);

	@Override
	public HashMap<String, HashMap<String, String>>  propertyFilter(String family, String proper_name,
			String compareOp, int type, String threshold, Scan scan) throws IOException {

		System.out.println("in the propertyFilter....");
	
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> res = new ArrayList<KeyValue>();
		HashMap<String, HashMap<String, String>>  result = new HashMap<String, HashMap<String, String>> ();
		boolean hasMoreResult = false;
		try {
			do {
				hasMoreResult = scanner.next(res);
				String source = getValue(res,family,proper_name);
				
				if(null != source ){
					if(Common.doCompare(type, source, compareOp, threshold)){
						String id = Bytes.toString(res.get(0).getRow());
						HashMap<String,String> row = new HashMap<String,String>();
						for(KeyValue kv: res){						
							row.put(kv.getKeyString(), Bytes.toString(kv.getValue()));								
						}
						result.put(id, row);
					}
				}
				res.clear();
				
			} while (hasMoreResult);
		} finally {
			scanner.close();
		}
		System.out.println("the result......" + result.size());
		return result;
	}
	
	private String getValue(List<KeyValue> res,String family,String proper_name){
		String key = family+":"+proper_name;
		for(KeyValue kv: res){			
			if(kv.getKey().equals(key)){
				return Bytes.toString(kv.getValue());
			}
		}
		return null;
	}
	
	
	/****************************************************************
	 * *********************For Schema1*****************************
	 ***************************************************************/
	public ArrayList<String> getUniqueCoprocs4S1(long s1, long s2,Scan scan) throws IOException {

		System.out.println("start to get unique in schema 1 between "+s1+" and "+s2);

		RegionScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> res = new ArrayList<KeyValue>();	
		ArrayList<String> result = new ArrayList<String>();
		boolean hasMoreResult = false;
		System.out.println("*********Region information:"
				+ scanner.getRegionInfo().getRegionNameAsString());
		int num = 0;
		try {			
			do {
				hasMoreResult = scanner.next(res);	
				if(res!=null){
					if(isUnique(res,s1,s2)){						
						result.add(Bytes.toString(res.get(0).getRow()));
					}
					num++;
				}	
				res.clear();
			} while (hasMoreResult);

		} finally {
			scanner.close();
		}

		System.out.println("total_number=>"+num+";result=>" + result.size());
		return result;

	}
	////kv map : {timestamp=24, family=t, qualifier=x, row=map-2-12345, vlen=6}
	private boolean isUnique(List<KeyValue> res,long s1, long s2){		
		long[] versions = {0,0};
		Map<String,Object> map = null;
		for(KeyValue kv:res){			
			map = kv.toStringMap();
			long timestamp = (Long)map.get("timestamp");		
			if(timestamp == s1){
				versions[0] = 1;
			}else if(timestamp == s2){
				versions[1] = 1;
			}else{
				throw new RuntimeException("other timestamps are returned");
			}
			if((versions[0]==1) && (versions[1]==1))
				break;
		}		
		return ((versions[0] == 1) && (versions[1] == 0));		
	}
	
	

	/****************************************************************
	 * *********************For Schema2*****************************
	 ***************************************************************/

	public ArrayList<String> getUniqueCoprocs4S2(long s1, long s2,
			Scan scan) throws IOException {

		System.out.println("start to get unique in schema2 between "+s1+" and "+s2);

		RegionScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> res = new ArrayList<KeyValue>();
		ArrayList<String> result = new ArrayList<String>();
		ArrayList<String> other = new ArrayList<String>();		
		System.out.println("*********Region information:"+ scanner.getRegionInfo().getRegionNameAsString());
		
		boolean hasMoreResult = false;
		try {
			do {
				hasMoreResult = scanner.next(res);
				for (KeyValue kv : res) {
					String key = Bytes.toString(kv.getRow());
					if (key.startsWith((String.valueOf(s1) + "-"))) {
						result.add(key);
					} else if (key.startsWith(String.valueOf(s2) + "-")) {
						other.add(key);
					}
					break;
				}
				res.clear();
			} while (hasMoreResult);			
			if(result != null && result.size()>0){
				result.removeAll(other);
			}
		} finally {
			scanner.close();
		}
		System.out.println("the result......" + result.size());

		return result;
	}

}
