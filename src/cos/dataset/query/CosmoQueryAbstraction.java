package cos.dataset.query;

import java.awt.Point;

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
			hbaseUtil.getTableHandler(tableName);
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
	public abstract void getUnique(int type, long s1,long s2,
								 	String[] result_families, String[] result_columns);
		
	// Q5: Return all particles whose property X changes from S1 to S2
	// filter(intersect(s1,s2))
	public abstract void intersectFilter(String proper_name, long s1,long s2);
	
	//Q3: Return all particles of type T within distance R of point P whose property X is above a threshold computed at timestep S1
	
}
