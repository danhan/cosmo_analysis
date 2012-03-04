package cos.dataset.query;

import java.awt.Point;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import cos.dataset.parser.CosmoConstant;
import cos.dataset.query.coprocessor.CosmoProtocol;

/*
 * Schema 1: row key: (type-particleId), family:column(pp:pos_x,pp:pos_y....) version: snapshot)
 * In this row key, it cannot include the space-indexing, because the location for each particle is changing, 
 * so space-indexing is changed over snapshot
 */
public class CosmoQuerySchema1 extends CosmoQueryAbstraction {

	public CosmoQuerySchema1() {
		tableName = CosmoConstant.TABLE_NAME;
		familyName = new String[]{CosmoConstant.FAMILY_NAME};
		try{
			this.setHBase();	
		}catch(Exception e){
			e.printStackTrace();
		}		
	}

	// Q1 : Return all particles whose property X is above a given threshold at
	// step S1
	@Override
	public void propertyFilter(String particleType,String family, String proper_name,
			String compareOp, int type,String threshold, long snapshot,
			String[] result_families, String[] result_columns) {
		ResultScanner rScanner = null;
		System.out.println("for snapshot: " + snapshot);
		try {
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(snapshot);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			filterList.addFilter(timeStampFilter);
			Filter rowFilter = hbaseUtil.getRowFilter("=", "^("+particleType+"-)");	
			filterList.addFilter(rowFilter);

			long s_time = System.currentTimeMillis();

			rScanner = this.hbaseUtil.getResultSet(filterList, result_families,result_columns);	
			
			HashMap<String, HashMap<String, String>> key_values = this.hbaseUtil.columnFilter(rScanner,family,proper_name,compareOp,
						type,threshold,result_families,result_columns);					
			
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			
			for(String key: key_values.keySet()){
				//System.out.print(key+"\t");
				HashMap<String,String> map = key_values.get(key);
				for(String item: map.keySet()){
					//System.out.print(item+"\t"+)
				}
			}
			
			// TODO store the time into database
			System.out.println("exe_time" + "\t" + "num_of_row");
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

	/*
	 *  Q4: Return gas particles destroyed between step S1 and S2(non-Javadoc)
	 *  it cannot add the column filter if you want to get values in all versions
	 * @see cos.dataset.query.CosmoQueryAbstraction#getUnique(int, long, long)
	 */
	@Override
	public void getUnique(int type, long s1, long s2) {
		ResultScanner rScanner = null;

		try {
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter rowFilter = hbaseUtil.getRowFilter("=", "(" + type + "-)");			
			fList.addFilter(rowFilter);			
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
						//System.out.println("key set : "+columns.keySet().toString());
						//System.out.println("values : "+columns.values().toString());
						for (byte[] column : columns.keySet()) {
							NavigableMap<Long, byte[]> values = columns.get(column);
							//System.out.println("navigable map: keyset "+values.keySet().toString());							
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
			System.out.println("exe_time" + "\t" + "result_row"+"\t"+"total_num_of_row");
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
	
/**************************************************************
 * 	*****************Coprocessor Client************************
 **************************************************************/
	
	public HashMap<String, HashMap<String,String>> propertyFilterCoprocs(final String particleType,final String family,final String proper_name,
			final String compareOp, final int dataType, final String threshold, long snapshot,
			final String[] result_families, final String[] result_columns){
		
		try{
		    // Call back class definition
		    class CosmoCallBack implements Batch.Callback<HashMap<String, HashMap<String,String>>> {
		    	HashMap<String, HashMap<String,String>> res = new HashMap<String, HashMap<String,String>>();

		      @Override
		      public void update(byte[] region, byte[] row, HashMap<String, HashMap<String,String>> result) {
		        res = result;
		      }
		    }
		    
		    CosmoCallBack callBack = new CosmoCallBack();
		   
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);			
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(snapshot);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			fList.addFilter(timeStampFilter);
			Filter rowFilter = hbaseUtil.getRowFilter("=", "^("+particleType+"-)");	
			fList.addFilter(rowFilter);
			
		    final Scan scan = hbaseUtil.generateScan(fList, result_families, result_columns);
		    System.out.println("start to send the query.....");

		    long s_time = System.currentTimeMillis();
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, null, null,
		    		new Batch.Call<CosmoProtocol, HashMap<String, HashMap<String,String>>>() {
		      public HashMap<String, HashMap<String,String>> call(CosmoProtocol instance)
		          throws IOException {  
		    	  System.out.println("in the call function");
		        return instance.propertyFilter(family,proper_name,compareOp,dataType,threshold,scan);			        
		        
		      };
		    }, callBack);	
		    
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time" + "\t" + "result_row");
			System.out.println(exe_time + "\t" + callBack.res.size());
		    
		    return callBack.res;
		    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}
		
		return null;
	    		
	}

	/*
	 * If want to get all versions, cannot specify the columns and families
	 * return a collection of particles ids
	 */
	public ArrayList<String> getUniqueCoprocs(final int type, final long s1, final long s2) {
		
		try{		    	    
		    // Call back class definition
		    class CosmoCallBack implements Batch.Callback<ArrayList<String>> {
		    	ArrayList<String> res = new ArrayList<String>();

		      @Override
		      public void update(byte[] region, byte[] row, ArrayList<String> result) {
		        res = result;
		      }
		    }		    
		    CosmoCallBack callBack = new CosmoCallBack();
		    

			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter rowFilter = hbaseUtil.getRowFilter("=", "(" + type + "-)");			
			fList.addFilter(rowFilter);
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(s1);
			timestamps.add(s2);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			fList.addFilter(timeStampFilter);

			final Scan scan = this.hbaseUtil.generateScan(fList, null,null);		    
	    
		    long s_time = System.currentTimeMillis();		    
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, null,null,
		    		new Batch.Call<CosmoProtocol, ArrayList<String>>() {
		      public ArrayList<String> call(CosmoProtocol instance)
		          throws IOException {
		        return instance.getUniqueCoprocs4S1(s1,s2,scan);
		      };
		    }, callBack);	
		    
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time" + "\t" + "result_row");
			System.out.println(exe_time + "\t" + callBack.res.size());
		    
		    return callBack.res;
		    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}
		
		return null;			
		
		
	}	
	

}
