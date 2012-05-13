package cos.dataset.query;

import java.util.ArrayList;
import java.util.HashMap;

import util.hbase.HBaseUtil;
import util.octree.XOctPoint;

public abstract class CosmoQueryAbstraction {
	
	HBaseUtil hbaseUtil = null;
	String tableName = "";
	String familyName[] = null;
	final int cacheSize = 5000;
			
	
	protected void setHBase() throws Exception{
		if(familyName == null)
			throw new Exception("family Name should be set first");
		if(tableName == null)
			throw new Exception("table name should be set first");	
		
		try{
			hbaseUtil = new HBaseUtil(null);
			hbaseUtil.getTableHandler(tableName);
			hbaseUtil.setScanConfig(cacheSize, true);
		}catch(Exception e){
			if(hbaseUtil != null)
				hbaseUtil.closeTableHandler();
			e.printStackTrace();
		}
	}	
	
	
}
