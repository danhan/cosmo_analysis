package cos.dataset.query;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import util.octree.XOctPoint;

import cos.dataset.parser.CosmoConstant;
import cos.dataset.query.coprocessor.CosmoProtocol;
import cos.dataset.space.analysis.SpaceQuadTreeIndexing;

/*
 * Schema 1: row key: (type-particleId), family:column(pp:pos_x,pp:pos_y....) version: snapshot)
 * In this row key, it cannot include the space-indexing, because the location for each particle is changing, 
 * so space-indexing is changed over snapshot
 */
public class CosmoQuerySchema1 extends CosmoQueryTime {

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
			Filter rowFilter = hbaseUtil.getPrefixFilter(particleType+"-");	
			filterList.addFilter(rowFilter);

			long s_time = System.currentTimeMillis();

			rScanner = this.hbaseUtil.getResultSet(null,filterList, result_families,result_columns,timestamps.size());	
			
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
			System.out.println("exe_time=>"+exe_time+";result=>"+ key_values.size());			
						
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rScanner != null)
				rScanner.close();
			hbaseUtil.closeTableHandler();
		}
	}


	/*
	 *  Q4: Return gas particles destroyed between step S1 and S2(non-Javadoc)
	 *  it cannot add the column filter if you want to get values in all versions
	 * @see cos.dataset.query.CosmoQueryAbstraction#getUnique(int, long, long)
	 */
	@Override
	public void getUnique(int particleType, long s1, long s2) {
		ResultScanner rScanner = null;

		try {
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter rowFilter = hbaseUtil.getPrefixFilter(particleType + "-");			
			fList.addFilter(rowFilter);			
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(s1);
			timestamps.add(s2);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			fList.addFilter(timeStampFilter);

			long s_time = System.currentTimeMillis();

			rScanner = this.hbaseUtil.getResultSet(null,fList, new String[]{"pp"},new String[]{"px"},timestamps.size());

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
						//System.out.println("DEBUG: key set : "+columns.keySet().toString());
						//System.out.println("DEBUG: values : "+columns.values().toString());
						for (byte[] column : columns.keySet()) {
							NavigableMap<Long, byte[]> values = columns.get(column);
							//System.out.println("DEBUG: navigable map: keyset "+values.keySet().toString());							
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
			System.out.println("exe_time=>" + exe_time+";total_num=>"+count+";result=>"+particles.size());
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rScanner != null)
				rScanner.close();
			hbaseUtil.closeTableHandler();
		}

	}

	/*
	 * (non-Javadoc)
	 * @see cos.dataset.query.CosmoQueryAbstraction#changeTrend(java.lang.String, java.lang.String[], java.util.ArrayList, java.lang.String, java.lang.String)
	 */
	@Override
	public HashMap<String,HashMap<Long,String>> changeTrend(String particleType, String[] particles, ArrayList time_series,String family,String column){
		
		ResultScanner rScanner = null;	
		HashMap<String,HashMap<Long,String>> returnValues = new HashMap<String,HashMap<Long,String>>();
		try {
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(time_series);
			filterList.addFilter(timeStampFilter);
			
			FilterList pFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			for(int i=0;i<particles.length;i++){				
				particles[i] = CosmoConstant.IndexFormatter.format(Long.valueOf(particles[i])); // change the particles
				//System.out.println(particles[i]);
				Filter rowFilter = hbaseUtil.getBinaryFilter("=", particleType+"-"+particles[i]);	
				pFilter.addFilter(rowFilter);
			}			
			filterList.addFilter(pFilter);
			
			// prepare the result container
			for(int i=0;i<particles.length;i++){
				String particle = particles[i];				
				HashMap<Long,String> snapshots = new HashMap<Long,String>();
				returnValues.put(particle, snapshots);
			}
						

			long s_time = System.currentTimeMillis();

			rScanner = this.hbaseUtil.getResultSet(null,filterList, new String[] {family},new String[]{column},time_series.size());							
			

			int count = 0;
						
			for (Result result : rScanner) {							
				String key = Bytes.toString(result.getRow());
				String particle = key.substring(key.indexOf('-')+1,key.length());				
				HashMap<Long,String> snapshots = returnValues.get(particle);				
				List<KeyValue> keyValues = result.getColumn(family.getBytes(), column.getBytes());				
				for(int i=0;i<keyValues.size();i++){
					long timestamp = keyValues.get(i).getTimestamp();
					String value = Bytes.toString(keyValues.get(i).getValue());					
					snapshots.put(timestamp, value);
					count++;
				}				
			}			
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+count);				
						
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rScanner != null)
				rScanner.close();
			hbaseUtil.closeTableHandler();
		}	
		
		for(String pid:returnValues.keySet()){				
			for(Long sid: returnValues.get(pid).keySet()){
				System.out.println(pid+"\t"+sid+"\t"+returnValues.get(pid).get(sid));	
			}
			
		}		
		
		return returnValues;
	}
	
/**************************************************************
 * 	*****************Coprocessor Client************************
 **************************************************************/
	
	public  ArrayList<String> copPropertyFilter(final String particleType,final String family,final String proper_name,
			final String compareOp, final int dataType, final String threshold, long snapshot,
			final String[] result_families, final String[] result_columns){
		
		try{			
		    // Call back class definition
		    class CosmoCallBack implements Batch.Callback< ArrayList<String> > {
		    	 ArrayList<String>  res = new  ArrayList<String> ();
		    	int count = 0;

		      @Override
		      public void update(byte[] region, byte[] row,  ArrayList<String> result) {
		    	  System.out.println((count++)+": come back region: "+Bytes.toString(region)+"; result: "+result.size());
		    	  res.addAll(result); // to verify the error when large data
		      }
		      
		    }
		    
		    CosmoCallBack callBack = new CosmoCallBack();
		   
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);			
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(snapshot);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			fList.addFilter(timeStampFilter);
			Filter rowFilter = hbaseUtil.getPrefixFilter(particleType+"-");	
			fList.addFilter(rowFilter);		
			
			String[] rowRanges= this.getRowRange(Integer.valueOf(particleType));
			
		    final Scan scan = hbaseUtil.generateScan(rowRanges,fList, new String[]{family}, new String[]{proper_name},1);		    
		    
		    System.out.println("start to send the query to coprocessor.....");

		    long s_time = System.currentTimeMillis();
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<CosmoProtocol,  ArrayList<String> >() {
		      public  ArrayList<String> call(CosmoProtocol instance)
		          throws IOException {  
		    	  
		        return instance.propertyFilter(family,proper_name,compareOp,dataType,threshold,scan);			        
		        
		      };
		    }, callBack);
		    
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+callBack.res.size());			    	
			
		    return callBack.res;
		    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbaseUtil.closeTableHandler();
		}
		
		return null;
	    		
	}

	/*
	 * If want to get all versions, cannot specify the columns and families
	 * return a collection of particles ids
	 */
	public ArrayList<String> copGetUnique(final int type, final long s1, final long s2) {
		
		try{		    	    
		    // Call back class definition
		    class CosmoCallBack implements Batch.Callback<ArrayList<String>> {
		    	int count = 0;
		    	ArrayList<String> res = new ArrayList<String>();

		      @Override
		      public void update(byte[] region, byte[] row, ArrayList<String> result) {
		        System.out.println((count++)+": come back region: "+Bytes.toString(region)+"; result: "+result.size());
		    	  res.addAll(result);
		      }
		    }		    
		    CosmoCallBack callBack = new CosmoCallBack();

			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter rowFilter = hbaseUtil.getPrefixFilter(type + "-");			
			fList.addFilter(rowFilter);
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(s1);
			timestamps.add(s2);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			fList.addFilter(timeStampFilter);
			String[] rowRanges = this.getRowRange(type);

			final Scan scan = this.hbaseUtil.generateScan(rowRanges,fList, new String[]{"pp"},new String[]{"px"},timestamps.size());							
			
		    long s_time = System.currentTimeMillis();		    
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<CosmoProtocol, ArrayList<String>>() {
		      public ArrayList<String> call(CosmoProtocol instance)
		          throws IOException {
		        return instance.getUniqueCoprocs4S1(s1,s2,scan);
		      };
		    }, callBack);	
		    
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+callBack.res.size());			
		    
		    return callBack.res;
		    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbaseUtil.closeTableHandler();
		}
		
		return null;			
				
	}	
	
	/*
	 * 		(non-Javadoc)
	 * @see cos.dataset.query.CosmoQueryAbstraction#changeTrendCop(java.lang.String, java.lang.String, java.util.ArrayList)
	 */
	public HashMap<String, HashMap<Long, String>> copChangeTrend(final String particleType, String[] particles, final ArrayList time_series,String family,String column){
		
		System.out.println("get change trend for paticles"+particles.toString()+" during "+time_series.toString());
		
		try{		    	    
		    // Call back class definition
		    class CosmoCallBack implements Batch.Callback< HashMap<String, HashMap<Long, String>>> {
		    	 HashMap<String, HashMap<Long, String>> res = new HashMap<String, HashMap<Long, String>>();
		    	 int count = 0;

		      @Override
		      public void update(byte[] region, byte[] row,  HashMap<String, HashMap<Long, String>> result) {
		    	  System.out.println((count++)+": come back region: "+Bytes.toString(region)+"; result: "+result.size());
		    	  for(String p: result.keySet()){
		    		  if(res.containsKey(p)){
		    			  res.get(p).putAll(res.get(p));		    			  		    			  
		    		  }else{		    			  
		    			  res.put(p,result.get(p));
		    		  }
		    	  }	
		      }
		    }		    
		    CosmoCallBack callBack = new CosmoCallBack();
		    
		    //set all filters
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(time_series);
			filterList.addFilter(timeStampFilter);
			Filter prefixFilter = hbaseUtil.getPrefixFilter(particleType+"-");	
			filterList.addFilter(prefixFilter);
			
			FilterList pFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			for(int i=0;i<particles.length;i++){
				particles[i] = CosmoConstant.IndexFormatter.format(Long.valueOf(particles[i]));
				Filter rowFilter = hbaseUtil.getBinaryFilter("=", particleType+"-"+particles[i]);	
				pFilter.addFilter(rowFilter);
			}
			filterList.addFilter(pFilter);
			
			//to get the start and stop row
			Arrays.sort(particles);
			String[] rowRanges = new String[1];
			rowRanges[0] = particleType+"-"+particles[0];
			Filter stopFilter = this.hbaseUtil.getInclusiveFilter(particleType+"-"+particles[particles.length-1]);
			filterList.addFilter(stopFilter);
			
			final Scan scan = this.hbaseUtil.generateScan(rowRanges,filterList, new String[]{family},new String[]{column},time_series.size());		    
	    
			System.out.println("scan start & stop: "+Bytes.toString(scan.getStartRow())+"; "+particleType+"-"+particles[particles.length-1]);
			
		    long s_time = System.currentTimeMillis();		    
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<CosmoProtocol,  HashMap<String, HashMap<Long, String>>>() {
		      public  HashMap<String, HashMap<Long, String>> call(CosmoProtocol instance)
		          throws IOException {
		        return instance.changeTrendCop4S1(scan);
		      };
		    }, callBack);	
		    
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+ exe_time + ";result=>"+callBack.res.size());			
		    
			int count = 0;
			for(String pid:callBack.res.keySet()){				
				for(Long sid: callBack.res.get(pid).keySet()){
					System.out.println((count++)+"\t"+pid+"\t"+sid+"\t"+callBack.res.get(pid).get(sid));	
				}				
			}			
		    return callBack.res;
		    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbaseUtil.closeTableHandler();
		}
		
		return null;					
	}
	
	private String[] getRowRange(int pType){
		String[] rowRange = new String[2];
		if(pType == 0){
			rowRange[0] = pType+"-"+CosmoConstant.ENUM_GAS.getStartIndex();
			rowRange[1] = pType+"-"+CosmoConstant.ENUM_GAS.getStopIndex();
		}else if(pType == 1){
			rowRange[0] = pType+"-"+CosmoConstant.ENUM_DARK_MATTER.getStartIndex();
			rowRange[1] = pType+"-"+CosmoConstant.ENUM_DARK_MATTER.getStopIndex();
		}else if(pType == 2){
			rowRange[0] = pType+"-"+CosmoConstant.ENUM_STAR.getStartIndex();
			rowRange[1] = pType+"-"+CosmoConstant.ENUM_STAR.getStopIndex();			
		}
		return rowRange;
	}
	
/*
 * For test	***************************************
 */
	public void copGetSpecificParticle(final String particleType,final String family, final String proper_name,
			final String compareOp, final int dataType,final String threshold, long snapshot,
			String[] result_families, String[] result_columns){
		try{			
		    // Call back class definition
		    class CosmoCallBack implements Batch.Callback<List> {
		    	int count = 0;
		    	List res = new LinkedList<String>();

		      @Override
		      public void update(byte[] region, byte[] row, List result) {
		        System.out.println((count++) +"region come back: "+Bytes.toString(region)+"; result: "+result.size());
		    	res.addAll(result); // to verify the error when large data
		      }
		      
		    }
		    
		    CosmoCallBack callBack = new CosmoCallBack();
		   
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);			
			List<Long> timestamps = new LinkedList<Long>();
			timestamps.add(snapshot);
			Filter timeStampFilter = hbaseUtil.getTimeStampFilter(timestamps);
			fList.addFilter(timeStampFilter);
			Filter rowFilter = hbaseUtil.getPrefixFilter(particleType+"-");	
			fList.addFilter(rowFilter);		
			
			String[] rowRanges = this.getRowRange(Integer.valueOf(particleType));
		    final Scan scan = hbaseUtil.generateScan(rowRanges,fList, new String[]{family}, new String[]{proper_name},1);		    
		    
		    System.out.println("start to send the query to coprocessor.....");

		    long s_time = System.currentTimeMillis();
		    Map<byte[],List> results = hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<CosmoProtocol, List>() {
		      public List call(CosmoProtocol instance)
		          throws IOException {  
		    	  
		       return instance.getSpecificParticle(family,proper_name,compareOp,dataType,threshold,scan);			        
		        
		      };
		    });
		    
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+callBack.res.size());
			
			for(Map.Entry<byte[], List> entry : results.entrySet()){
				System.out.println("region=>"+Bytes.toString(entry.getKey()) + "; count=>"+entry.getValue().size());
			}
	    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbaseUtil.closeTableHandler();
		}
					
	}
	
}
