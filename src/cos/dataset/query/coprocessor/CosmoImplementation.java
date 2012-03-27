package cos.dataset.query.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import util.Common;

public class CosmoImplementation extends BaseEndpointCoprocessor implements
		CosmoProtocol {

	static final Log log = LogFactory.getLog(CosmoImplementation.class);

	
	// for both of them, because their is what   
	public List  getSpecificParticle(String family, String proper_name,
			String compareOp,int dataType, String threshold,Scan scan) throws IOException{
		
		System.out.println("in the getSpecificParticle....");
		
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> res = new ArrayList<KeyValue>();
		List<String> result = new LinkedList<String>();
		boolean hasMoreResult = false;
		long start = System.currentTimeMillis();
		try {
			do {
				hasMoreResult = scanner.next(res);
				//System.out.println("resource : "+res.toString());
				
				String source = this.getValue(res,family,proper_name);
				//System.out.println(family+":"+proper_name+"="+source);
				
				if(null != source ){
					if(Common.doCompare(dataType, source, compareOp, threshold)){
						String id = Bytes.toString(res.get(0).getRow());
						result.add(id);				
					}
				}
				res.clear();
				
			} while (hasMoreResult);
		} finally {
			scanner.close();
		}
		System.out.println("exe_time=>"+(System.currentTimeMillis()-start));
		System.out.println("the result......" + result.size());
		
		return result;		
	}
	
	
	@Override
	public  ArrayList<String>  propertyFilter(String family, String proper_name,
			String compareOp, int dataType, String threshold, Scan scan) throws IOException {
		
		long start = System.currentTimeMillis();
		System.out.println(start+": in the propertyFilter....");
		
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> res = new ArrayList<KeyValue>();
		ArrayList<String> result = new ArrayList<String>();
		boolean hasMoreResult = false;
		
		int count = 0;
		try {
			do {
				hasMoreResult = scanner.next(res);
				//System.out.println("resource : "+res.toString());
				
				String source = this.getValue(res,family,proper_name);
				//System.out.println(family+":"+proper_name+"="+source);
				
				if(null != source ){
					if(Common.doCompare(dataType, source, compareOp, threshold)){
						String id = Bytes.toString(res.get(0).getRow());
						result.add(id);
					}
				}
				count++;
				res.clear();
				
			} while (hasMoreResult);
		} finally {
			scanner.close();
		}	
		System.out.println("exe_time=>"+(System.currentTimeMillis()-start)+";count=>"+count+";result=>"+result.size());		
		
		return result;
	}
	
	private String getValue(List<KeyValue> res,String family,String proper_name){		
		for(KeyValue kv: res){				
			log.debug(kv.toStringMap().toString());		
			if(Bytes.toString(kv.getQualifier()).equals(proper_name) && Bytes.toString(kv.getFamily()).equals(family)){
				return Bytes.toString(kv.getValue());				
			}
		}
		return null;
	}		
	
	/****************************************************************
	 * *********************For Schema1*****************************
	 ***************************************************************/
	public ArrayList<String> getUniqueCoprocs4S1(long s1, long s2,Scan scan) throws IOException {

		long start = System.currentTimeMillis();
		System.out.println(start+ ": start to get unique in schema 1 between "+s1+" and "+s2);

		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> res = new ArrayList<KeyValue>();	
		ArrayList<String> result = new ArrayList<String>();
		boolean hasMoreResult = false;

		int num = 0;
		
		try {			
			do {
				hasMoreResult = scanner.next(res);	
				//System.out.println("result: "+res.toString());
				if(res!=null && res.size()>0){
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
		System.out.println("exe_time=>"+(System.currentTimeMillis()-start)+";total_number=>"+num+";result=>" + result.size());		
		return result;

	}
	////kv map : {timestamp=24, family=t, qualifier=x, row=map-2-12345, vlen=6}
	private boolean isUnique(List<KeyValue> res,long s1, long s2){		
		long[] versions = {0,0};
		Map<String,Object> map = null;
		for(KeyValue kv:res){			
			map = kv.toStringMap();
			//System.out.println("map is "+map);
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
		//System.out.println("result is: "+versions[0]+";"+versions[1]);
		return ((versions[0] == 1) && (versions[1] == 0));		
	}
	
	public HashMap<String, HashMap<Long, String>> changeTrendCop4S1(Scan scan)throws IOException {
		
		long start = System.currentTimeMillis();			
		System.out.println(start + ": S1 start to get changeTrend for type ");
		
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> keyValues = new ArrayList<KeyValue>();			
		boolean hasMoreResult = false;
		
		int num = 0;
		HashMap<String, HashMap<Long, String>>  result = new HashMap<String, HashMap<Long, String>> ();
		
		try {			
			do {
				hasMoreResult = scanner.next(keyValues);
				
				if(keyValues!=null && keyValues.size()>0){	
					// this is one row, but with many columns and timestamps value
					String pid = Bytes.toString(keyValues.get(0).getRow());
					HashMap<Long,String> snapshots = new HashMap<Long,String>();
					for(int i=0;i<keyValues.size();i++){
						long timestamp = keyValues.get(i).getTimestamp();
						String value = Bytes.toString(keyValues.get(i).getValue());					
						snapshots.put(timestamp, value);	
						num++;
					}	
					result.put(pid, snapshots);
			
				}	
				keyValues.clear();
				
			} while (hasMoreResult);

		} finally {
			scanner.close();
		}

		System.out.println("exe_time=>"+(System.currentTimeMillis()-start)+";total_number=>"+num+";result=>" + result.size());		
		return result;

	}		
	
	

	/****************************************************************
	 * *********************For Schema2*****************************
	 ***************************************************************/

	public HashMap<String,String> getUniqueCoprocs4S2(String s1, String s2,
			Scan scan) throws IOException {

		long start = System.currentTimeMillis();
		
		System.out.println(start+ ": start to get unique in schema2 between "+s1+" and "+s2);

		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		
		List<KeyValue> res = new ArrayList<KeyValue>();

		HashMap<String,String> particles = new HashMap<String,String>();
		
		boolean hasMoreResult = false;
		
		int count = 0;
		try {
			do {
				hasMoreResult = scanner.next(res);
				for (KeyValue kv : res) {
					String key = Bytes.toString(kv.getRow());
//					System.out.println("key: "+key);
					String tokens[] = key.split("-");
					String pid = tokens[2];
					String snapshot = tokens[0];
					if(snapshot.equals(s1) || snapshot.equals(s2)){
						if(particles.containsKey(pid)){
							particles.remove(pid);
							//System.out.println("particle "+tokens[2]+" is in both snapshots");
						}else{
							particles.put(pid, snapshot);	
							//System.out.println("add the new particle "+pid);
						}						
					}
						
					break;
				}	
				count++;
				res.clear();
				
			} while (hasMoreResult);
			
		} finally {
			scanner.close();
		}
		System.out.println("exe_time=>"+(System.currentTimeMillis()-start)+";total_number=>"+count+";result=>"+particles.size());				
		

		return particles;
	}
	
	
	public HashMap<String, HashMap<Long, String>> changeTrendCop4S2(Scan scan)throws IOException {
		
		long start = System.currentTimeMillis();
		System.out.println(start+": S2 start to get changeTrend for type ");
		
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> keyValues = new ArrayList<KeyValue>();			
		boolean hasMoreResult = false;
		
		int num = 0;
		HashMap<String, HashMap<Long, String>>  result = new HashMap<String, HashMap<Long, String>>();
		
		try {			
			do {
				hasMoreResult = scanner.next(keyValues);	
				if(keyValues!=null && keyValues.size()>0){					
					String key = Bytes.toString(keyValues.get(0).getRow());
					String items[] = key.split("-");
					long snapshot = Long.parseLong(items[0]);
					String p = items[2];
					String value = Bytes.toString(keyValues.get(0).getValue());
					if(result.containsKey(p)){
						result.get(p).put(snapshot, value);
					}else{
						HashMap<Long,String> snaps = new HashMap<Long,String>();
						snaps.put(snapshot, value);
						result.put(p, snaps);
					}			
					num++;
				}	
				keyValues.clear();
			} while (hasMoreResult);

		} finally {
			scanner.close();
		}
		System.out.println("exe_time=>"+(System.currentTimeMillis()-start)+";total_number=>"+num+";result=>" + result.size());				
		return result;

	}		
	
	public void test4PropertyFilter(String family, String proper_name,
			String compareOp,int dataType, String threshold,Scan scan) throws IOException{
		
	}

}
