package cos.dataset.query;

import java.awt.Point;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

public class CosmoQuerySchema1 extends CosmoQueryAbstraction{
	
	public CosmoQuerySchema1(int schema){
		super(1);
	}

	// Q1 : Return all particles whose property X is above a given threshold at step S1
	@Override
	public void propertyFilter(String family,String proper_name, String compareOp,
								String threshold, long snapshot,
									String[] result_families, String[] result_columns) {	
		ResultScanner rScanner = null;
		System.out.println("for snapshot: "+snapshot);		
		try{
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter columnFilter = hbaseUtil.getColumnFilter(family, proper_name, compareOp, threshold);									
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(snapshot);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			fList.addFilter(columnFilter);	
			fList.addFilter(timeStampFilter);
				
			
			long s_time = System.currentTimeMillis();

			rScanner = this.hbaseUtil.getResultSet(fList,result_families,result_columns);
			
			HashMap<String,HashMap<String,String>> key_values = new HashMap<String,HashMap<String,String>>();
			int count = 0;
			for(Result result: rScanner){
				count++;
				HashMap<String,String> oneRow = new HashMap<String,String>();
				String key = Bytes.toString(result.getRow());	
				
				if (null != result_columns){
					for(int i=0;i<result_columns.length;i++){
						byte[] value = result.getValue(result_families[i].getBytes(), result_columns[i].getBytes());
						
						oneRow.put(result_columns[i], Bytes.toString(value));						
					}
					key_values.put(key, oneRow);
				}else{
					for(KeyValue kv:result.raw()){																		
						oneRow.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
					}
					key_values.put(key, oneRow);
				}
				if(count<5){					
					for(String k:key_values.keySet()){
						System.out.println("key=>"+key);
						HashMap<String,String> kv = key_values.get(k);
						for(String q: kv.keySet()){
							System.out.print(q+"=>"+kv.get(q)+"; ");
						}
					}
					System.out.println();
					
				}
					
				// TODO store them into files
				key_values.clear();
				
			}
			
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			//TODO store the time into database
			System.out.print("exe_time"+"\t"+"num_of_row"+"\n");	
			System.out.println(exe_time+"\t"+count);			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(rScanner != null) 
				rScanner.close();
		}
		
		
	}

	@Override
	public void findNeigbour(Point p, int type, int distance) {
		// TODO Auto-generated method stub
		
	}

	// Q4: Return gas particles destroyed between step S1 and S2
	@Override
	public void getUnique(int type, long s1,long s2,
							String[] result_families, String[] result_columns) {
		ResultScanner rScanner = null;
		
		try{
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);								
			Filter rowFilter = hbaseUtil.getRowFilter("=", "(-"+type+"-)");
			fList.addFilter(rowFilter);
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(s1);
			timestamps.add(s2);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			fList.addFilter(timeStampFilter);	
			
					
			long s_time = System.currentTimeMillis();

			rScanner = this.hbaseUtil.getResultSet(fList,null,null);
			
			HashMap<String,HashMap<String,String>> key_values = new HashMap<String,HashMap<String,String>>();
			int count = 0;
			for(Result result: rScanner){
				count++;
				HashMap<String,String> oneRow = new HashMap<String,String>();
				String key = Bytes.toString(result.getRow());	
				
				for(KeyValue cell: result.raw()){
					long timestamp = cell.getTimestamp();
					//result.
				}
				if (null != result_columns){
					for(int i=0;i<result_columns.length;i++){
						byte[] value = result.getValue(result_families[i].getBytes(), result_columns[i].getBytes());
						
						oneRow.put(result_columns[i], Bytes.toString(value));						
					}
					key_values.put(key, oneRow);
				}else{
					for(KeyValue kv:result.raw()){																		
						oneRow.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
					}
					key_values.put(key, oneRow);
				}
				if(count<5){					
					for(String k:key_values.keySet()){
						System.out.println("key=>"+key);
						HashMap<String,String> kv = key_values.get(k);
						for(String q: kv.keySet()){
							System.out.print(q+"=>"+kv.get(q)+"; ");
						}
					}
					System.out.println();
					
				}
					
				// TODO store them into files
				key_values.clear();
				
			}
			
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			//TODO store the time into database
			System.out.print("exe_time"+"\t"+"num_of_row"+"\n");	
			System.out.println(exe_time+"\t"+count);			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(rScanner != null) 
				rScanner.close();
		}
		
	}

	@Override
	public void intersectFilter(String proper_name, long s1, long s2) {
		// TODO Auto-generated method stub
		
	}

}
