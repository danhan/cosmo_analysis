package cos.dataset.query;

import java.awt.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
 * Schema 2: row key: (snapshot-type-particleId), family:column(pp:pos_x,pp:pos_y....) no version support)
 * In this row key, it cannot include the space-indexing, because the location for each particle is changing, 
 * so space-indexing is changed over snapshot. The space-indexing is not useful here. 
 */
public class CosmoQuerySchema2 extends CosmoQueryAbstraction{

	
	public CosmoQuerySchema2() {
		tableName = CosmoConstant.TABLE_NAME_2;
		familyName = new String[]{CosmoConstant.FAMILY_NAME};
		try{
			this.setHBase();	
		}catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	
	@Override
	public void propertyFilter(String particleType,String family, String proper_name,
			String compareOp, int type,String threshold, long snapshot,
			String[] result_families, String[] result_columns) {
		try{
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);				
			Filter rowFilter = hbaseUtil.getRowFilter("=", "^("+snapshot+"-"+particleType+"-)");	
			fList.addFilter(rowFilter);
						
			long s_time = System.currentTimeMillis();

			ResultScanner rScanner = this.hbaseUtil.getResultSet(fList,result_families,result_columns);
			
			HashMap<String, HashMap<String, String>> key_values = this.hbaseUtil.columnFilter(rScanner,family,proper_name,compareOp,
					type,threshold,result_families,result_columns);				
			
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
			
			ArrayList<String> s1_particles = new ArrayList<String>();
			ArrayList<String> s2_particles = new ArrayList<String>();
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
			s1_particles.removeAll(s2_particles);			
					
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
	public  HashMap<String, HashMap<String,String>> propertyFilterCoprocs(final String particleType,final String family, final String proper_name,
			final String compareOp, final int type, final String threshold, long snapshot,
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
			Filter rowFilter = hbaseUtil.getRowFilter("=", "^("+snapshot+"-"+particleType+"-)");			
			fList.addFilter(rowFilter);			    		    
		    final Scan scan = hbaseUtil.generateScan(fList, result_families, result_columns);
		    
		    
		    long s_time = System.currentTimeMillis();
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, null, null,
		    		new Batch.Call<CosmoProtocol, HashMap<String, HashMap<String,String>>>() {
		      public HashMap<String, HashMap<String,String>> call(CosmoProtocol instance)
		          throws IOException {
		        return instance.propertyFilter(family,proper_name,compareOp,type,threshold,scan);
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
			String regex = "(^"+s1+"-|^"+s2+"-)";
			Filter rowFilter = hbaseUtil.getRowFilter("=", regex);
			fList.addFilter(rowFilter);			
			Filter keyOnlyFilter = hbaseUtil.getKeyOnlyFilter();
			fList.addFilter(keyOnlyFilter);
			
			byte[][] rowRange = new byte[][]{ Bytes.toBytes((s1<s2?s1:s2)+"-"), Bytes.toBytes((s1>s2?s1:s2)+"-"+Long.MAX_VALUE)	};			   		    
		    final Scan scan = hbaseUtil.generateScan(rowRange,fList, null,null);
		    
		    long s_time = System.currentTimeMillis();		    
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, rowRange[0],rowRange[1],
		    		new Batch.Call<CosmoProtocol, ArrayList<String>>() {
		      public ArrayList<String> call(CosmoProtocol instance)
		          throws IOException {
		        return instance.getUniqueCoprocs4S2(s1,s2,scan);
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
