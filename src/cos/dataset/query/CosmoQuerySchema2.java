package cos.dataset.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import util.octree.X3DPoint;

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
		ResultScanner rScanner = null;
		try{
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);				
			Filter rowFilter = hbaseUtil.getPrefixFilter(CosmoConstant.snapshotFormatter.format(snapshot)+"-"+particleType+"-");	
			fList.addFilter(rowFilter);			
						
			long s_time = System.currentTimeMillis();

			rScanner = this.hbaseUtil.getResultSet(null,fList,result_families,result_columns,1);
			
			HashMap<String, HashMap<String, String>> key_values = this.hbaseUtil.columnFilter(rScanner,family,proper_name,compareOp,
					type,threshold,result_families,result_columns);				
			
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+key_values.size());			
			
		}catch(Exception e){
			e.printStackTrace();
		}finally {
			if (rScanner != null)
				rScanner.close();
			hbaseUtil.closeTableHandler();
		}			
		
	}

	@Override
	public void findNeigbour(X3DPoint p,double distance,long snapshot) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void getUnique(int type, long s1, long s2) {
		ResultScanner rScanner = null;
		final String snap1 = CosmoConstant.snapshotFormatter.format(s1);
		final String snap2 = CosmoConstant.snapshotFormatter.format(s2);
		try{
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);			
			String regex = "^("+snap1+"-|"+snap2+"-)";
			Filter rowFilter = hbaseUtil.getRegrexRowFilter("=", regex);
			fList.addFilter(rowFilter);				
			Filter keyOnlyFilter = hbaseUtil.getKeyOnlyFilter();
			fList.addFilter(keyOnlyFilter);			
		
			
			String rowRanges[] = new String[2];

			if(snap1.compareTo(snap2)>0){
				rowRanges[0] = this.getRowRange(s2, type)[0];
				rowRanges[1] = this.getRowRange(s1, type)[1];
			}else{
				rowRanges[0] = this.getRowRange(s1, type)[0];
				rowRanges[1] = this.getRowRange(s2, type)[1];
			}	

			System.out.println("scan start & stop row index: "+rowRanges[0]+"; "+rowRanges[1]);
			
			long s_time = System.currentTimeMillis();		
			
			rScanner = this.hbaseUtil.getResultSet(rowRanges,fList,null,null,-1);
			
			ArrayList<String> s1_particles = new ArrayList<String>();
			ArrayList<String> s2_particles = new ArrayList<String>();
			int count=0;
			for (Result result : rScanner) {
				count++;
				String key = Bytes.toString(result.getRow());				
				if(key.startsWith((String.valueOf(snap1)+"-"))){
					s1_particles.add(key.substring(key.lastIndexOf('-'),key.length()));
				}else if(key.startsWith(String.valueOf(snap2)+"-")){
					s2_particles.add(key.substring(key.lastIndexOf('-'),key.length()));
				}
			}			
			
			System.out.println("total number: "+count +"; "+s1_particles.size() + ";"+s2_particles.size());
			s1_particles.removeAll(s2_particles);			
					
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";total_num=>"+count+";result=>"+s1_particles.size());			
			
		}catch(Exception e){
			e.printStackTrace();
		}finally {
			if (rScanner != null)
				rScanner.close();
			hbaseUtil.closeTableHandler();
		}					
	}

	@Override
	public HashMap<String,HashMap<Long,String>> changeTrend(String particleType, String[] particles, ArrayList time_series,String family,String column){
		
		ResultScanner rScanner = null;
		HashMap<String,HashMap<Long,String>> returnValues = new HashMap<String,HashMap<Long,String>>();
		try {
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			
			for(int s=0;s<time_series.size();s++){
				long snapshot = (Long)time_series.get(s);
				for(int p=0;p<particles.length;p++){
					particles[p] = CosmoConstant.IndexFormatter.format(Long.valueOf(particles[p]));
					String rowKey = CosmoConstant.snapshotFormatter.format(snapshot)+"-"+particleType+"-"+particles[p];
					Filter rowFilter = hbaseUtil.getBinaryFilter("=",rowKey);	
					filterList.addFilter(rowFilter);
				}
			}		

			// prepare the result container
			for(int i=0;i<particles.length;i++){
				String particle =particles[i];				
				HashMap<Long,String> snapshots = new HashMap<Long,String>();
				returnValues.put(particle, snapshots);
			}
			
			long s_time = System.currentTimeMillis();

			rScanner = this.hbaseUtil.getResultSet(null,filterList, new String[]{family},new String[]{column},1);							
			
			int count = 0;
			for (Result result : rScanner) {					
				String key = Bytes.toString(result.getRow());
				String items[] = key.split("-");
				long snapshot = Long.parseLong(items[0]);
				String p = items[2];
				String value = Bytes.toString(result.getColumn(family.getBytes(), column.getBytes()).get(0).getValue());
				returnValues.get(p).put(snapshot, value);	
				count++;
			}	
			
			long e_time = System.currentTimeMillis();
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+count);			
			
			for(String pid:returnValues.keySet()){				
				for(Long sid: returnValues.get(pid).keySet()){
					System.out.println(pid+"\t"+sid+"\t"+returnValues.get(pid).get(sid));	
				}				
			}				
						
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rScanner != null)
				rScanner.close();
			hbaseUtil.closeTableHandler();
		}
		return returnValues;
	}
	
/*********************************************************************************
 * *******************************Coprocessor*************************************	
 ********************************************************************************/
	public  ArrayList<String> copPropertyFilter(final String particleType,final String family, final String proper_name,
			final String compareOp, final int type, final String threshold, long snapshot,
			final String[] result_families, final String[] result_columns){
		
		System.out.println("in copPropertyFilter");
		try{		    	    
		    // Call back class definition
		    class CosmoCallBack implements Batch.Callback<ArrayList<String>> {
		    	int count = 0;
		    	ArrayList<String> res = new ArrayList<String>();

		      @Override
		      public void update(byte[] region, byte[] row, ArrayList<String> result) {
		    	  System.out.println((count++)+"region come back: "+Bytes.toString(region)+"; result: "+result.size());
		    	  res.addAll(result);
		      }
		    }
		    
		    CosmoCallBack callBack = new CosmoCallBack();
		    
		    
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);			
			Filter rowFilter = hbaseUtil.getRegrexRowFilter("=", "^("+CosmoConstant.snapshotFormatter.format(snapshot)+"-"+particleType+"-)");			
			fList.addFilter(rowFilter);	
			
			String[] rowRanges = this.getRowRange(snapshot, Integer.valueOf(particleType));					
			
		    final Scan scan = hbaseUtil.generateScan(rowRanges,fList, result_families, result_columns,-1);
		    
		    System.out.println("scan start & stop: "+Bytes.toString(scan.getStartRow())+"; "+Bytes.toString(scan.getStopRow()));
		    
		    long s_time = System.currentTimeMillis();
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<CosmoProtocol, ArrayList<String>>() {
		      public ArrayList<String> call(CosmoProtocol instance)
		          throws IOException {
		        return instance.propertyFilter(family,proper_name,compareOp,type,threshold,scan);
		      };
		    }, callBack);	
		    
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+ callBack.res.size());			
		    
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
	
	public ArrayList<String> copGetUnique(final int type, final long s1, final long s2) {
		
		System.out.println("get unique with Coprocessor,particle type "+type+";"+s1+"<-->"+s2);
		ArrayList<String> diff = new ArrayList<String>(); 
		final String snap1 = CosmoConstant.snapshotFormatter.format(s1);
		final String snap2 = CosmoConstant.snapshotFormatter.format(s2);		
		try{		    	    
		    // Call back class definition
		    class CosmoCallBack implements Batch.Callback<HashMap<String,String>> {
		    	int count = 0;
		    	HashMap<String,String> res = new HashMap<String,String>();

		      @Override
		      public void update(byte[] region, byte[] row, HashMap<String,String> result) {
		    	  System.out.println((count++)+"region come back: "+Bytes.toString(region)+"; result: "+result.size());
		    	  for(String p: result.keySet()){		        	  	        	 
		        	  if(res.containsKey(p)){
		        		  res.remove(p);		        		  
		        	  }else{
		        		  res.put(p, result.get(p));		        		  	        			 
		        	  }		        	  
		          }		    	  
		      }		      
		    }		    
		    CosmoCallBack callBack = new CosmoCallBack();
		    
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);			
			String regex = "^(" + snap1+"-"+type+"-"+"|"+snap2+"-"+type+"-)";
			Filter rowFilter = hbaseUtil.getRegrexRowFilter("=", regex);
			fList.addFilter(rowFilter);			
			Filter keyOnlyFilter = hbaseUtil.getKeyOnlyFilter();
			fList.addFilter(keyOnlyFilter);
			
			String rowRanges[] = new String[2];

			if(snap1.compareTo(snap2)>0){
				rowRanges[0] = this.getRowRange(s2, type)[0];
				rowRanges[1] = this.getRowRange(s1, type)[1];
			}else{
				rowRanges[0] = this.getRowRange(s1, type)[0];
				rowRanges[1] = this.getRowRange(s2, type)[1];
			}			
						
			
			final Scan scan = hbaseUtil.generateScan(rowRanges,fList, null,null,-1);
		   		    
			System.out.println("scan start & stop: "+Bytes.toString(scan.getStartRow())+"; "+Bytes.toString(scan.getStopRow()));
			
		    long s_time = System.currentTimeMillis();		    
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<CosmoProtocol, HashMap<String,String> >() {
		      public HashMap<String,String> call(CosmoProtocol instance)
		          throws IOException {
		        return instance.getUniqueCoprocs4S2(snap1,snap2,scan);
		      };
		    }, callBack);
		    
		    //value are the snapshot, the result contains all the particles unique in both snapshot
		    for(String p: callBack.res.keySet()){
		    	String value = callBack.res.get(p);		    	
		    	if(value.equals(snap1)){
		    		diff.add(p);		    		
		    	}
		    }
	    
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+diff.size());			
		    
		    return diff;
		    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbaseUtil.closeTableHandler();
		}
		
		return null;			
				
	}
	
	
	public  HashMap<String, HashMap<Long, String>> copChangeTrend(final String particleType, String[] particles, final ArrayList snapshots,String family,String column){
		
		System.out.println("get change trend for paticles"+particles.toString()+" during "+snapshots.toString());
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
		    			  res.get(p).putAll(result.get(p));		    			  		    			  
		    		  }else{
		    			  res.put(p,result.get(p));
		    		  }
		    	  }		    	  		    	  
		      }
		    }		    
		    CosmoCallBack callBack = new CosmoCallBack();
		    
		    FilterList managerFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		    //set all filters for rows
		    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);			
			for(int s=0;s<snapshots.size();s++){
				long snapshot = (Long)snapshots.get(s);
				for(int p=0;p<particles.length;p++){
					particles[p] = CosmoConstant.IndexFormatter.format(Long.valueOf(particles[p]));
					String rowKey = CosmoConstant.snapshotFormatter.format(snapshot)+"-"+particleType+"-"+particles[p];
					Filter rowFilter = hbaseUtil.getBinaryFilter("=",rowKey);	
					filterList.addFilter(rowFilter);
				}
			}
			managerFilter.addFilter(filterList);
			
			//to get the start and stop row, and the stop row should be included
			String[] tmp = new String[snapshots.size()];
			for(int t=0;t<snapshots.size();t++){
				tmp[t] = CosmoConstant.snapshotFormatter.format(snapshots.get(t));
			}						
			Arrays.sort(tmp);			
			Arrays.sort(particles);
			String[] rowRanges = new String[1];
			rowRanges[0] = tmp[0]+"-"+particleType+"-"+particles[0];
			Filter stopFilter = this.hbaseUtil.getInclusiveFilter(tmp[tmp.length-1]+"-"+particleType+"-"+particles[particles.length-1]);
			
			managerFilter.addFilter(stopFilter);

			
			final Scan scan = this.hbaseUtil.generateScan(rowRanges,managerFilter, new String[]{family},new String[]{column},snapshots.size());		    
	    
			System.out.println("scan start & stop: "+Bytes.toString(scan.getStartRow())+"; "+tmp[tmp.length-1]+"-"+particleType+"-"+particles[particles.length-1]);
			
		    long s_time = System.currentTimeMillis();		    
		    hbaseUtil.getHTable().coprocessorExec(CosmoProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<CosmoProtocol,  HashMap<String, HashMap<Long, String>>>() {
		      public  HashMap<String, HashMap<Long, String>> call(CosmoProtocol instance)
		          throws IOException {
		        return instance.changeTrendCop4S2(scan);
		      };
		    }, callBack);	
		    
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+ callBack.res.size());			
		    
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
	
	private String[] getRowRange(long snapshot,int pType){
		String[] rowRange = new String[2];		
		if(pType == 0){
			rowRange[0] = CosmoConstant.snapshotFormatter.format(snapshot)+"-"+pType+"-"+CosmoConstant.ENUM_GAS.getStartIndex();
			rowRange[1] = CosmoConstant.snapshotFormatter.format(snapshot)+"-"+pType+"-"+CosmoConstant.ENUM_GAS.getStopIndex();
		}else if(pType == 1){
			rowRange[0] = CosmoConstant.snapshotFormatter.format(snapshot)+"-"+pType+"-"+CosmoConstant.ENUM_DARK_MATTER.getStartIndex();
			rowRange[1] = CosmoConstant.snapshotFormatter.format(snapshot)+"-"+pType+"-"+CosmoConstant.ENUM_DARK_MATTER.getStopIndex();
		}else if(pType == 2){
			rowRange[0] = CosmoConstant.snapshotFormatter.format(snapshot)+"-"+pType+"-"+CosmoConstant.ENUM_STAR.getStartIndex();
			rowRange[1] = CosmoConstant.snapshotFormatter.format(snapshot)+"-"+pType+"-"+CosmoConstant.ENUM_STAR.getStopIndex();			
		}
		return rowRange;
	}
	
	
}
