package cos.dataset.query;

import java.awt.Point;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

public class CosmoQuerySchema1 extends CosmoQueryAbstraction {

	public CosmoQuerySchema1(int schema) {
		super(1);
	}

	// Q1 : Return all particles whose property X is above a given threshold at
	// step S1
	@Override
	public void propertyFilter(String family, String proper_name,
			String compareOp, String threshold, long snapshot,
			String[] result_families, String[] result_columns) {
		ResultScanner rScanner = null;
		System.out.println("for snapshot: " + snapshot);
		try {
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter rowFilter = hbaseUtil.getColumnFilter(family,
					proper_name, compareOp, threshold);
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(snapshot);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			fList.addFilter(rowFilter);
			fList.addFilter(timeStampFilter);

			long s_time = System.currentTimeMillis();

			rScanner = this.hbaseUtil.getResultSet(fList, result_families,result_columns);
			
			HashMap<String, HashMap<String, String>> key_values = displayScanResult(rScanner,result_families,result_columns);
			
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			
			// TODO store the time into database
			System.out.print("exe_time" + "\t" + "num_of_row" + "\n");
			System.out.println(exe_time + "\t" + key_values.size());			
						
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rScanner != null)
				rScanner.close();
		}

	}

	@Override
	public void findNeigbour(Point p, int type, int distance) {
		// TODO Auto-generated method stub

	}

	// Q4: Return gas particles destroyed between step S1 and S2
	@Override
	public void getUnique(int type, long s1, long s2) {
		ResultScanner rScanner = null;

		try {
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter rowFilter = hbaseUtil.getRowFilter("=", "(-" + type + "-)");
			Filter columnFilter = hbaseUtil.getFirstColumnFilter();
			fList.addFilter(rowFilter);
			fList.addFilter(columnFilter);
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(s1);
			timestamps.add(s2);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			fList.addFilter(timeStampFilter);

			long s_time = System.currentTimeMillis();

			rScanner = this.hbaseUtil.getResultSet(fList, null, null);

			List<String> particles = new LinkedList<String>();
			int count = 0;
			for (Result result : rScanner) {
				count++;
				String key = Bytes.toString(result.getRow());
				boolean s1Unique = false;
				NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = result
						.getMap();
				if (resultMap != null) {
					for (byte[] family : resultMap.keySet()) {						
						NavigableMap<byte[], NavigableMap<Long, byte[]>> columns = resultMap.get(family);
						for (byte[] column : columns.keySet()) {
							NavigableMap<Long, byte[]> values = columns.get(column);
							if (values.keySet().contains(s1) && !values.keySet().contains(s2)) {
								s1Unique = true;
								break;
							} else {
								break;
							}
						}						
						break;
					}
				}
				if (s1Unique) {
					particles.add(key);
				}
			}
			for (int i = 0; i < particles.size(); i++) {
				// System.out.println(particles.get(i)+";");
			}			

			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.print("exe_time" + "\t" + "result_row"+"\t"+"total_num_of_row" + "\n");
			System.out.println(exe_time + "\t" + particles.size()+"\t"+count);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rScanner != null)
				rScanner.close();
		}

	}

	@Override
	public void intersectFilter(String proper_name, long s1, long s2) {
		// TODO Auto-generated method stub

	}

}
