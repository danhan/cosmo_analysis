package cos.dataset.query;

import java.awt.Point;
import java.util.Collection;
import java.util.HashMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import cos.dataset.parser.CosmoConstant;

import util.hbase.HBaseUtil;

public abstract class CosmoQueryAbstraction {
	
	HBaseUtil hbaseUtil = null;
	String tableName = "";
	String familyName[] = null;
	final int cacheSize = 1000;
	
	public CosmoQueryAbstraction(int schema){
		try{
			hbaseUtil = new HBaseUtil(null);
			String tableName = null;
			if(schema==1){
				tableName = CosmoConstant.TABLE_NAME;
				familyName = new String[]{CosmoConstant.FAMILY_NAME};
			}else if(schema==2){
				tableName = CosmoConstant.TABLE_NAME_2;
				familyName = new String[]{CosmoConstant.FAMILY_NAME};
			}else if(schema==3){
				
			}
			hbaseUtil.getTableHandler("test");//(tableName);
			hbaseUtil.setScanConfig(cacheSize, false);
			
		}catch(Exception e){				
			if(hbaseUtil != null)
				hbaseUtil.closeTableHandler();
			e.printStackTrace();
		}
	}	
	
	//Q1 : Return all particles whose property X is above a given threshold at step S1
	public abstract void propertyFilter(String family,String proper_name, String compareOp,
										String threshold, long snapshot,
											String[] result_families, String[] result_columns);
	
	//Q2: Return all particles of type T within distance R of point P,go through all snapshots?
	public abstract void findNeigbour(Point p, int type,int distance);
	
	// Q4: Return gas particles destroyed between step S1 and S2
	// s1-(intersect(s1,s2))
	public abstract void getUnique(int type, long s1,long s2);
		
	// Q5: Return all particles whose property X changes from S1 to S2
	// filter(intersect(s1,s2))
	public abstract void intersectFilter(String proper_name, long s1,long s2);
	
	//Q3: Return all particles of type T within distance R of point P whose property X is above a threshold computed at timestep S1
	
	protected HashMap<String, HashMap<String, String>> displayScanResult(ResultScanner rScanner,
								String[] result_families, String[] result_columns) throws Exception{
		HashMap<String, HashMap<String, String>> key_values = null;
		int count = 0;
		if(rScanner == null)
			throw new Exception("rScanner is null");
		try{
			key_values = new HashMap<String, HashMap<String, String>>();
			for (Result result : rScanner) {
				count++;
				HashMap<String, String> oneRow = new HashMap<String, String>();
				String key = Bytes.toString(result.getRow());

				if (null != result_columns) {
					for (int i = 0; i < result_columns.length; i++) {
						byte[] value = result.getValue(
								result_families[i].getBytes(),
								result_columns[i].getBytes());

						oneRow.put(result_columns[i], Bytes.toString(value));
					}
					key_values.put(key, oneRow);
				} else {
					for (KeyValue kv : result.raw()) {
						oneRow.put(Bytes.toString(kv.getQualifier()),
								Bytes.toString(kv.getValue()));
					}
					key_values.put(key, oneRow);
				}
				
				// TODO store them into files
				if (count < 5) {
					for (String k : key_values.keySet()) {
						System.out.println("key=>" + key);
						HashMap<String, String> kv = key_values.get(k);
						for (String q : kv.keySet()) {
							System.out.print(q + "=>" + kv.get(q) + "; ");
						}
					}
					System.out.println();
				}				

			}
		
		}catch(Exception e){
			e.printStackTrace();
		}

		return key_values;

	}
	
	
	
}
