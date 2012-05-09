package cos.dataset.experiment;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;


import cos.dataset.parser.CosmoConstant;
import cos.dataset.query.CosmoQueryTime;

public abstract class QueryClientBase {

	CosmoQueryTime queryEngine = null;
	Properties configure;

	abstract CosmoQueryTime getQueryEngine();

	private static String where = null;
	private static String left_join = null;
	private int num_of_run = -1;

	public QueryClientBase() {

		configure = new Properties();
		try {
			
			File f = new File("./conf/tests.properties");
			if(f.exists()){
				configure.load(new FileInputStream("./conf/tests.properties"));
			}else{
				configure.load(new FileInputStream("../conf/tests.properties"));
			}
		
		} catch (IOException e) {
			e.printStackTrace();
		}
		where = configure.getProperty("where").trim();
		left_join = configure.getProperty("left_join").trim();

		if (configure.getProperty("times_of_run") != null)
			num_of_run = Integer.valueOf(configure.getProperty("times_of_run"));

		queryEngine = getQueryEngine();

	}

	public void warmExec(){
		this.exePropertyFilter("where");
		this.exeGetUnique("left_join");
		this.exePropertyFilterCorpcs("where");
		this.exeGetUniqueCoprocs("left_join");
	}
	
	
	// This is only for one time for each query, no cache at all
	public void execute(int coprocs,int query, String property) {		
	
		this.num_of_run = 1;
		if(coprocs == 0){
			if (query == 1) {
				this.exePropertyFilter(property);
			} else if (query == 2) {
				this.exeGetUnique(property);
			}else if(query == 3){
				this.exeGetNeighbor(property);
			}else if(query == 4){
				this.exeChangeTrends(property);
			}
		}else if(coprocs == 1){
			if (query == 1) {
				this.exePropertyFilterCorpcs(property);
			} else if (query == 2) {
				this.exeGetUniqueCoprocs(property);
			}else if(query == 3){
				this.exeGetNeighborCop(property);
			}else if(query == 4){
				this.exeChangeTrendsCop(property);
			}				
		}
	}

	private void exePropertyFilter(String property_name) {
		String property = configure.getProperty(property_name);
		System.out.println(property);
		String[] items = property.split(";");
		String pType = items[0];
		String family = items[1];
		String column = items[2];
		String compareOp = items[3];
		String threshold = items[4];
		long snapshot = Long.parseLong(items[5]);
		int type = CosmoConstant.COSMO_DATA_TYPE_FLOAT;
		for(int i=0;i<this.num_of_run;i++){
			this.queryEngine.propertyFilter(pType,family, column, compareOp, type,threshold, snapshot, new String[]{family}, new String[]{column});	
		}
		
	}

	private void exeGetUnique(String property_name) {
		String property = configure.getProperty(property_name);
		System.out.println(property);
		String[] items = property.split(";");
		int type = Integer.parseInt(items[0]);
		long s1 = Long.parseLong(items[1]);
		long s2 = Long.parseLong(items[2]);
		for(int i=0;i<this.num_of_run;i++){
			this.queryEngine.getUnique(type, s1, s2);	
		}
	}
	
	private void exeGetNeighbor(String property_name) {
//		String property = configure.getProperty(property_name);
//		System.out.println(property);
//		String[] items = property.split(";");		
//		float x = Float.parseFloat(items[0]);
//		float y = Float.parseFloat(items[1]);
//		float z = Float.parseFloat(items[2]);
//		double distance = Double.parseDouble(items[3]);
//		long snapshot = Long.parseLong(items[4]);
//		for(int i=0;i<this.num_of_run;i++){
//			this.queryEngine.findNeigbour(new X3DPoint(x,y,z,-1), distance, snapshot);	
//		}
	}	

	/*
	 * trend=2;pp;eps;[33554434,33554444,33554454];[24,84,128]
	 */
	private void exeChangeTrends(String property_name) {
		String property = configure.getProperty(property_name);
		System.out.println(property);
		String[] items = property.split(";");	
		
		String type = items[0];
		String family = items[1];
		String column = items[2];
		// it is an interval of particles
		items[3] = items[3].substring(items[3].indexOf('[')+1,items[3].indexOf(']'));
		String pid_interval[] = items[3].split(",");
		Long begin_pid = Long.valueOf(pid_interval[0]);
		int count = Integer.valueOf(pid_interval[1]);
		String[] pid_list = new String[count];
		for(int i=0;i<count;i++){
			pid_list[i] = String.valueOf((begin_pid+i));			
		}
		
		// it is a vector for snapshots
		items[4] = items[4].substring(items[4].indexOf('[')+1,items[4].indexOf(']'));
		String[] series = items[4].split(",");
		ArrayList<Long> time_series = new ArrayList<Long>();
		for(String s: series){
			time_series.add(Long.parseLong(s));
		}
		for(int i=0;i<this.num_of_run;i++){
			this.queryEngine.changeTrend(type, pid_list,time_series,family,column);	
		}
	}	
	

	/************************ Coprocessor *************************************************/

	private void exePropertyFilterCorpcs(String property_name) {
		String property = configure.getProperty(property_name);
		System.out.println(property);
		String[] items = property.split(";");
		String pType = items[0];
		String family = items[1];
		String column = items[2];
		String compareOp = items[3];
		String threshold = items[4];
		long snapshot = Long.parseLong(items[5]);
		int type = CosmoConstant.COSMO_DATA_TYPE_FLOAT;
		for(int i=0;i<this.num_of_run;i++){
			this.queryEngine.copPropertyFilter(pType,family, column, compareOp, type,threshold, snapshot,new String[]{family}, new String[]{column});		
		}
		
	}

	private void exeGetUniqueCoprocs(String property_name) {
		String property = configure.getProperty(property_name);
		System.out.println(property);
		String[] items = property.split(";");
		int type = Integer.parseInt(items[0]);
		long s1 = Long.parseLong(items[1]);
		long s2 = Long.parseLong(items[2]);
		for(int i=0;i<this.num_of_run;i++){
			this.queryEngine.copGetUnique(type, s1, s2);	
		}
	}
	
	private void exeGetNeighborCop(String property_name) {
		//TODO
//		String property = configure.getProperty(property_name);
//		System.out.println(property);
//		String[] items = property.split(";");		
//		float x = Float.parseFloat(items[0]);
//		float y = Float.parseFloat(items[1]);
//		float z = Float.parseFloat(items[2]);
//		double distance = Double.parseDouble(items[3]);
//		long snapshot = Long.parseLong(items[4]);
//		for(int i=0;i<this.num_of_run;i++){
//			this.queryEngine.findNeigbour(new X3DPoint(x,y,z,-1), distance, snapshot);	
//		}
	}	
	
	private void exeChangeTrendsCop(String property_name) {
		
		String property = configure.getProperty(property_name);
		System.out.println(property);
		String[] items = property.split(";");	
		
		String type = items[0];
		String family = items[1];
		String column = items[2];
		//it is an interval
		// it is an interval of particles
		items[3] = items[3].substring(items[3].indexOf('[')+1,items[3].indexOf(']'));
		String pid_interval[] = items[3].split(",");
		Long begin_pid = Long.valueOf(pid_interval[0]);
		int count = Integer.valueOf(pid_interval[1]);
		String[] pid_list = new String[count];
		for(int i=0;i<count;i++){
			pid_list[i] = String.valueOf((begin_pid+i));
		}
	
		
		items[4] = items[4].substring(items[4].indexOf('[')+1,items[4].indexOf(']'));
		String[] series = items[4].split(",");
		ArrayList<Long> time_series = new ArrayList<Long>();
		for(String s: series){
			time_series.add(Long.parseLong(s));
		}
		for(int i=0;i<this.num_of_run;i++){
			this.queryEngine.copChangeTrend(type, pid_list,time_series,family,column);	
		}
	}	
	
	
	
}
