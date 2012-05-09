package cos.dataset.query;

import java.io.BufferedReader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import cos.dataset.parser.CosmoConstant;
import cos.dataset.space.analysis.SpaceRasterIndexing;
import cos.dataset.space.analysis.X3Point;
import cos.dataset.space.analysis.XCube;

public class CosmoQuerySchema5 extends CosmoQuerySpace{
	
	public CosmoQuerySchema5() {
		tableName = CosmoConstant.TABLE_NAME_5;
		familyName = new String[]{CosmoConstant.FAMILY_NAME};
		try{
			this.setHBase();	
		}catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	public static void main(String[] args){
		CosmoQuerySchema5 schema5 = new CosmoQuerySchema5();
		X3Point<Double> point = new X3Point<Double>(0.5779609,0.5161069,0.14541020);		
		schema5.findNeigbour(point, 0.3, 22);
	}
	
	
	/*
	 * 1 Get the neighbours in client side 
	 * 2 send the neighbours particles id and get all information about neighbours
	 * @see cos.dataset.query.CosmoQueryAbstraction#findNeigbour(util.octree.X3DPoint, int, double, long)
	 */
	@Override
	public void findNeigbour(X3Point<Double> p, double distance,long snapshot) {
		
		SpaceRasterIndexing rasterIndexing = new SpaceRasterIndexing(-1,1,-1,1,-1,1,CosmoConstant.LOCATION_OFFSET,
				1,1,CosmoConstant.SPACE_SCALE);
		
		XCube cube = rasterIndexing.getExteriorArea(p, distance);
		long[] box = rasterIndexing.getRange(cube);
		
		ResultScanner rScanner = null;

		try {
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
											
			List<Long> timestamps = new LinkedList<Long>();
			for(long b = box[4]; b<=box[5];b++)
				timestamps.add(b);
			
			System.out.println("timestamp is : "+timestamps.size());
			
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			//fList.addFilter(timeStampFilter);

			String[] rowRanges = new String[]{String.valueOf(box[2]),String.valueOf(box[3])};
			
			long s_time = System.currentTimeMillis();
			
			List<String> columns = new ArrayList<String>();
			for(long x=box[0];x<=box[1];x++)
				columns.add(String.valueOf(x));
			String[] c = new String[columns.size()];

			c = columns.toArray(c);

			
			rScanner = this.hbaseUtil.getResultSet(rowRanges,fList, new String[]{"pp"},null,-1);

			List<String> particles = new LinkedList<String>();
			int count = 0;
			for (Result result : rScanner) {
				count++;
				String key = Bytes.toString(result.getRow());
				System.out.println( "result: "+result.toString());
				particles.add(key);
			}
			
			for (int i = 0; i < particles.size(); i++) {
				// System.out.println(particles.get(i)+";");
			}			

			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>" + exe_time+";total_num=>"+count+";result=>"+particles.size());
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rScanner != null)
				rScanner.close();
			hbaseUtil.closeTableHandler();
		}		


	}
}
