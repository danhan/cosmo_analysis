package cos.dataset.query.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class CosmoImplementation extends BaseEndpointCoprocessor implements
		CosmoProtocol {

	static final Log log = LogFactory.getLog(CosmoImplementation.class);

	@Override
	public Map<String, String> propertyFilter(String[] result_families,
			String[] result_columns, Scan scan) throws IOException {

		/*
		 * if(result_columns != null){ for(int i=0;i<result_columns.length;i++){
		 * scan
		 * .addColumn(result_families[i].getBytes(),result_columns[i].getBytes
		 * ()); } }
		 */
		log.debug("in the propertyFilter....");
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> res = new ArrayList<KeyValue>();
		Map<String, String> result = new HashMap<String, String>();
		boolean hasMoreResult = false;
		try {
			do {
				hasMoreResult = scanner.next(res);
				for (KeyValue kv : res) {
					// log.debug("got a kv: " + kv);
					log.debug("get buffer : " + Bytes.toString(kv.getBuffer()));
					String id = Bytes.toString(kv.getRow());
					// log.debug("result to be added is: " + availBikes +
					// " id: " + id);
					result.put(id, Bytes.toString((kv.getValue())));
				}
				res.clear();
			} while (hasMoreResult);
		} finally {
			scanner.close();
		}
		log.debug("the result......" + result.size());
		return result;
	}

	/****************************************************************
	 * *********************For Schema1*****************************
	 ***************************************************************/
	public Collection<String> getUniqueCoprocs4S1(int type, long s1, long s2,
			Scan scan) throws IOException {

		System.out.println("in the propertyFilter....");

		//scan.setTimeRange(1, 50);
		scan.setMaxVersions(100);
		//scan.addFamily("t".getBytes());
		//scan.addColumn("t".getBytes(), "x".getBytes());
		
		RegionScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> res = new ArrayList<KeyValue>();
		HashMap<String,List<KeyValue>> backup = new HashMap<String,List<KeyValue>>();
		Collection<String> result = new ArrayList<String>();
		boolean hasMoreResult = false;
		System.out.println("*********Region information:"
				+ scanner.getRegionInfo().getRegionId());
		
	//if(scanner.isFilterDone()){
			try {
				int num = 0;
				do {
					hasMoreResult = scanner.next(res);
					List<KeyValue> pairs = new LinkedList<KeyValue>();
					String key = "";
					for (KeyValue kv : res) {
						key = Bytes.toString(kv.getRow());
						System.out.println("key is : "+key+";value=>"+Bytes.toString(kv.getValue()));
						pairs.add(kv);						
					}	
					num++;
					System.out.println("key: "+key+"; before adding size: "+backup.size()+";scanner num:"+num);
					backup.put(key, pairs);
					System.out.println("key: "+key+";after adding size: "+backup.size()+"");
				} while (hasMoreResult);

			} finally {
				scanner.close();
			}	
			KVComparator compare = new KVComparator().getComparatorIgnoringTimestamps();
			
			for(String key: backup.keySet()){
				List<KeyValue> kvList = backup.get(key);
				for(int i=0;i<kvList.size();i++){
					System.out.println("key=>"+key+";value=>"+kvList.toString());
					if(i+1<kvList.size()){
						System.out.println("compare===================");
						System.out.println("result: "+compare.compare(kvList.get(i),kvList.get(i+1)));	
					}
					
				}
			}			
		//}else{
			//System.out.println("filter is not done.....");
		//}


		log.debug("the result......" + result.size());

		return result;

	}

	/****************************************************************
	 * *********************For Schema2*****************************
	 ***************************************************************/

	public Collection<String> getUniqueCoprocs4S2(int type, long s1, long s2,
			Scan scan) throws IOException {

		log.debug("in the propertyFilter....");

		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> res = new ArrayList<KeyValue>();
		Collection<String> result = new ArrayList<String>();
		Collection<String> other = new ArrayList<String>();
		boolean hasMoreResult = false;
		try {
			do {
				hasMoreResult = scanner.next(res);
				for (KeyValue kv : res) {
					String key = Bytes.toString(kv.getRow());
					if (key.startsWith((String.valueOf(s1) + "-"))) {
						result.add(key.substring(key.lastIndexOf('-'),
								key.length()));
					} else if (key.startsWith(String.valueOf(s2) + "-")) {
						other.add(key.substring(key.lastIndexOf('-'),
								key.length()));
					}
				}
				res.clear();
			} while (hasMoreResult);

			boolean r = result.removeAll(other);
			if (!r)
				throw new IOException("the removeall is wrong");
		} finally {
			scanner.close();
		}

		log.debug("the result......" + result.size());

		return result;
	}

}
