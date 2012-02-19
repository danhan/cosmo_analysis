package cos.dataset.experiment;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import cos.dataset.parser.CosmoConstant;
import cos.dataset.query.CosmoQueryAbstraction;

public abstract class QueryClientBase {

	CosmoQueryAbstraction queryEngine = null;
	Properties configure;

	abstract CosmoQueryAbstraction getQueryEngine();

	private static String where = null;
	private static String left_join = null;
	private int num_of_run = -1;

	public QueryClientBase() {

		configure = new Properties();
		try {
			configure.load(new FileInputStream("tests.properties"));
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
			}			
		}else if(coprocs == 1){
			if (query == 1) {
				this.exePropertyFilterCorpcs(property);
			} else if (query == 2) {
				this.exeGetUniqueCoprocs(property);
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
			this.queryEngine.propertyFilter(pType,family, column, compareOp, type,
					threshold, snapshot, null, null);	
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
			this.queryEngine.propertyFilterCoprocs(pType,family, column, compareOp, type,
					threshold, snapshot, null, null);
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
			this.queryEngine.getUniqueCoprocs(type, s1, s2);	
		}
	}
	
}
