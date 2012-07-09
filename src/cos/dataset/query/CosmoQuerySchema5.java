package cos.dataset.query;

import java.io.BufferedReader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;

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
		schema5.findNeigbour(point, 0.4, 22);
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
		
		XCube<Long> cube = rasterIndexing.getExteriorArea(p, distance);
		//long[] box = rasterIndexing.getRange(cube);
		
		ResultScanner rScanner = null;

		try {
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
											
			List<Long> timestamps = new LinkedList<Long>();
			for(long b = cube.getFront(); b<=cube.getBack();b++)
				timestamps.add(b);
			
			System.out.println("timestamp is : "+timestamps.size());
			
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			//fList.addFilter(timeStampFilter);

			String[] rowRanges = new String[]{String.valueOf(cube.getBottom()),String.valueOf(cube.getTop())};
			
			long s_time = System.currentTimeMillis();
			
			List<String> columns = new ArrayList<String>();
			for(long x=cube.getLeft();x<=cube.getRight();x++)
				columns.add(String.valueOf(x));
			String[] c = new String[columns.size()];

			c = columns.toArray(c);

			
			rScanner = this.hbaseUtil.getResultSet(rowRanges,fList, new String[]{"pp"},null,-1);

			List<X3Point> points = new LinkedList<X3Point>();
			int count = 0;
			for (Result result : rScanner) {
				count++;
				String key = Bytes.toString(result.getRow());
				System.out.println( "result: "+result.toString());
				long x = Long.valueOf(key).longValue();
				long y = -1;
				long z = -1;
				NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = result.getMap();
				
				for (byte[] family : resultMap.keySet()) {						
					NavigableMap<byte[], NavigableMap<Long, byte[]>> fcolumns = resultMap.get(family);
					//System.out.println("DEBUG: key set : "+fcolumns.keySet().toString());
					//System.out.println("DEBUG: values : "+fcolumns.values().toString());
					for (byte[] qualifer : fcolumns.keySet()) {	
						System.out.println("qualifer: "+Bytes.toString(qualifer));
						y = Long.valueOf(Bytes.toString(qualifer));
						NavigableMap<Long, byte[]> values = fcolumns.get(qualifer);
						for(long version:values.keySet()){
							z = version;							
							X3Point<Long> p3D = new X3Point<Long>(x,y,z);
							p3D.setIndex(Bytes.toString(values.get(version)));
							points.add(p3D);
						}				
					}
				}
			}
			
			X3Point<Long> cellPoint = rasterIndexing.getCell4Point(p);
			List<String> tp = new LinkedList<String>(); // store the right point index
			int fp = 0;
			for (int i = 0; i < points.size(); i++) {
				//System.out.println(points.get(i)+";");
				X3Point<Long> tmp = points.get(i);
				double d = this.distance(cellPoint, tmp);
				
				if(d >= distance * CosmoConstant.SPACE_SCALE){
					System.out.println(d);
					fp++;
				}else{
					tp.add(tmp.getIndex());
				}
			}			
			
			Collections.sort(tp);
			System.out.println("false positive : "+fp + "; true positive: "+tp.size());
			System.out.println("true positive : "+tp.toString());

			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>" + exe_time+";total_num=>"+count+";result=>"+points.size());
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rScanner != null)
				rScanner.close();
			hbaseUtil.closeTableHandler();
		}		


	}
	
	private double distance(X3Point<Long> left, X3Point<Long> right){
		
		double distance = Math.sqrt(Math.pow(left.getX() - right.getX(), 2.0)
				+ Math.pow(left.getY() - right.getY(), 2.0)
				+ Math.pow(left.getZ()- right.getZ(), 2.0));
		
		return distance;
	}
	
}
