package cos.dataset.experiment;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import cos.dataset.parser.CosmoConstant;
import cos.dataset.query.CosmoQueryAbstraction;

public abstract class TestCaseBase {
	
	CosmoQueryAbstraction queryEngine = null;
	Properties configure;
	
	abstract CosmoQueryAbstraction getQueryEngine();
	

	
	private static String where = null;
	private static String left_join = null;
	private static int num_of_run = -1;
	
	public TestCaseBase() {		
		
		configure = new Properties();
		try {
			configure.load(new FileInputStream("tests.properties"));
		} catch (IOException e) {
			e.printStackTrace();		
		}				
		where = configure.getProperty("where").trim();	
		left_join = configure.getProperty("left_join").trim();
		
		if(configure.getProperty("times_of_run") != null)
			num_of_run = Integer.valueOf(configure.getProperty("times_of_run"));
		
		queryEngine = getQueryEngine();
				
	}
	
	private void exePropertyFilter(String property_name){
		String property = configure.getProperty(property_name);
		String[] items = property.split(";");
		String family = items[0];
		String column = items[1];
		String compareOp = items[2];
		String threshold = items[3];
		long snapshot = Long.parseLong(items[4]);
		int type = CosmoConstant.COSMO_DATA_TYPE_FLOAT;
		this.queryEngine.propertyFilter(family, column, compareOp, type, 
				threshold, snapshot, null, null);
			
	}
	
	private void exeGetUnique(String property_name){
		String property = configure.getProperty(property_name);
		String[] items = property.split(";");
		
	}
	
	
	
	
	

}
