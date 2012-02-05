package cos.dataset.query;

import java.awt.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import cos.dataset.query.coprocessor.CosmoProtocol;

public class CosmoQuerySchema2 extends CosmoQueryAbstraction{

	
	public CosmoQuerySchema2(int schema){
		super(2);
	}
	
	@Override
	public void propertyFilter(String family,String proper_name, String compareOp,
			String threshold, long snapshot,
			String[] result_families, String[] result_columns) {
		try{
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter rowFilter1 = hbaseUtil.getColumnFilter(family, proper_name, compareOp, threshold);	
			Filter rowFilter = hbaseUtil.getRowFilter("=", "^("+snapshot+"-)");
			fList.addFilter(rowFilter1);	
			fList.addFilter(rowFilter);
						
			long s_time = System.currentTimeMillis();

			ResultScanner rScanner = this.hbaseUtil.getResultSet(fList,result_families,result_columns);
			
			HashMap<String, HashMap<String, String>> key_values = displayScanResult(rScanner,result_families,result_columns);
			
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			
			// TODO store the time into database
			System.out.print("exe_time" + "\t" + "num_of_row" + "\n");
			System.out.println(exe_time + "\t" + key_values.size());
			
		}catch(Exception e){
			e.printStackTrace();
		}		
		
	}

	@Override
	public void findNeigbour(Point p, int type, int distance) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void getUnique(int type, long s1, long s2) {
		try{
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);			
			String regex = "(^"+s1+"-|^"+s2+"-)";
			Filter rowFilter = hbaseUtil.getRowFilter("=", regex);
			fList.addFilter(rowFilter);			
			Filter keyOnlyFilter = hbaseUtil.getKeyOnlyFilter();
			fList.addFilter(keyOnlyFilter);
			
			byte[][] rowRange = new byte[][]{ Bytes.toBytes((s1<s2?s1:s2)+"-"), Bytes.toBytes((s1>s2?s1:s2)+"-"+Long.MAX_VALUE)	};	
		
			long s_time = System.currentTimeMillis();
			
			ResultScanner rScanner = this.hbaseUtil.getResultSet(rowRange,fList,null,null);
			
			Collection<String> s1_particles = new ArrayList<String>();
			Collection<String> s2_particles = new ArrayList<String>();
			int count=0;
			for (Result result : rScanner) {
				count++;
				String key = Bytes.toString(result.getRow());				
				if(key.startsWith((String.valueOf(s1)+"-"))){
					s1_particles.add(key.substring(key.lastIndexOf('-'),key.length()));
				}else if(key.startsWith(String.valueOf(s2)+"-")){
					s2_particles.add(key.substring(key.lastIndexOf('-'),key.length()));
				}
			}			
			
			System.out.println("total number: "+count +"; "+s1_particles.size() + ";"+s2_particles.size());
			boolean result = s1_particles.removeAll(s2_particles);			
			
			if(!result)
				throw new Exception("the removeall is wrong");
			
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			
			// TODO store the time into database
			System.out.print("exe_time" + "\t" +"result_row"+"\t"+ "total_num_of_row" + "\n");
			System.out.println(exe_time + "\t" + s1_particles.size()+"\t"+count);
			
		}catch(Exception e){
			e.printStackTrace();
		}		
		
		
	}

	@Override
	public void intersectFilter(String proper_name, long s1, long s2) {
		// TODO Auto-generated method stub
		
	}
	
/*********************************************************************************
 * *******************************Coprocessor*************************************	
 ********************************************************************************/
	public  Map<String, String> propertyFilterCoprocs(String family, String proper_name,
			String compareOp, String threshold, long snapshot,
			final String[] result_families, final String[] result_columns){
		
		try{		    	    
		    // Call back class definition
		    class CosmoCallBack implements Batch.Callback<Map<String, String>> {
		      Map<String, String> res = new HashMap<String, String>();

		      @Override
		      public void update(byte[] region, byte[] row, Map<String, String> result) {
		        res = result;
		      }
		    }
		    
		    CosmoCallBack callBack = new CosmoCallBack();
		    
		    
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter rowFilter1 = hbaseUtil.getColumnFilter(family, proper_name, compareOp, threshold);	
			Filter rowFilter = hbaseUtil.getRowFilter("=", "^("+snapshot+"-)");
			fList.addFilter(rowFilter1);	
			fList.addFilter(rowFilter);			    		    
		    final Scan scan = hbaseUtil.generateScan(fList, result_families, result_columns);
		    
		    
		    long s_time = System.currentTimeMillis();
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, null, null,
		    		new Batch.Call<CosmoProtocol, Map<String, String>>() {
		      public Map<String, String> call(CosmoProtocol instance)
		          throws IOException {
		        return instance.propertyFilter(result_families, result_columns, scan);
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
	
	public Collection<String> getUniqueCoprocs(final int type, final long s1, final long s2) {
		
		try{		    	    
		    // Call back class definition
		    class CosmoCallBack implements Batch.Callback<Collection<String>> {
		      Collection<String> res = new ArrayList<String>();

		      @Override
		      public void update(byte[] region, byte[] row, Collection<String> result) {
		        res = result;
		      }
		    }		    
		    CosmoCallBack callBack = new CosmoCallBack();
		    
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);			
			String regex = "(^"+s1+"-|^"+s2+"-)";
			Filter rowFilter = hbaseUtil.getRowFilter("=", regex);
			fList.addFilter(rowFilter);			
			Filter keyOnlyFilter = hbaseUtil.getKeyOnlyFilter();
			fList.addFilter(keyOnlyFilter);
			
			byte[][] rowRange = new byte[][]{ Bytes.toBytes((s1<s2?s1:s2)+"-"), Bytes.toBytes((s1>s2?s1:s2)+"-"+Long.MAX_VALUE)	};			   		    
		    final Scan scan = hbaseUtil.generateScan(rowRange,fList, null,null);
		    
		    long s_time = System.currentTimeMillis();		    
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, rowRange[0],rowRange[1],
		    		new Batch.Call<CosmoProtocol, Collection<String>>() {
		      public Collection<String> call(CosmoProtocol instance)
		          throws IOException {
		        return instance.getUniqueCoprocs4S2(type,s1,s2,scan);
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
