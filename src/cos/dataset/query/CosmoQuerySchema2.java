package cos.dataset.query;

import java.awt.Point;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

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
			Filter columnFilter = hbaseUtil.getColumnFilter(family, proper_name, compareOp, threshold);	
			Filter rowFilter = hbaseUtil.getRowFilter(">", "^("+snapshot+")");
			fList.addFilter(columnFilter);	
			fList.addFilter(rowFilter);
						
			long s_time = System.currentTimeMillis();

			ResultScanner rScanner = this.hbaseUtil.getResultSet(fList,null,result_families,result_columns);
			
			HashMap<String,String> key_values = new HashMap<String,String>();
			for(Result result: rScanner){
				for(int i=0;i<result_columns.length;i++){
					byte[] value = result.getValue(result_families[i].getBytes(), result_columns[i].getBytes());
					key_values.put(result_columns[i], ByteBuffer.wrap(value).toString());
				}
				System.out.println(key_values.toString());
				// TODO store them into files
				key_values.clear();
				
			}
			
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			//TODO store the time into database
			System.out.println("execution time: "+exe_time);			
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
		// TODO Auto-generated method stub
		
	}

	@Override
	public void intersectFilter(String proper_name, long s1, long s2) {
		// TODO Auto-generated method stub
		
	}
	
	
	
}
