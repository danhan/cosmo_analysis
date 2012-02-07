package cos.dataset.query;

import java.awt.Point;
import java.util.ArrayList;
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
			
	
	protected void setHBase() throws Exception{
		if(familyName == null)
			throw new Exception("family Name should be set first");
		if(tableName == null)
			throw new Exception("table name should be set first");	
		
		try{
			hbaseUtil = new HBaseUtil(null);
			hbaseUtil.getTableHandler(tableName);
			hbaseUtil.setScanConfig(cacheSize, false);
		}catch(Exception e){
			if(hbaseUtil != null)
				hbaseUtil.closeTableHandler();
			e.printStackTrace();
		}
	}	
	
	//Q1 : Return all particles whose property X is above a given threshold at step S1
	public abstract void propertyFilter(String family, String proper_name,
			String compareOp, int type,String threshold, long snapshot,
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
	
	
	/*************************For Coprocessor**************************************************/
	
	public abstract HashMap<String, HashMap<String,String>> propertyFilterCoprocs(final String family,final String proper_name,
			final String compareOp, final int type, final String threshold, long snapshot,
			final String[] result_families, final String[] result_columns);
	
	
	public abstract ArrayList<String> getUniqueCoprocs(final int type, final long s1, final long s2);	
	
	
}
