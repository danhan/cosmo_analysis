package util.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;

import cos.dataset.parser.CosmoConstant;


public class HBaseUtil {

	public static final Log log = LogFactory.getLog(HBaseUtil.class);
	private Configuration conf = null;
	private HBaseAdmin admin = null;
	private HTable table = null;
	private int cacheSize = -1;
	private boolean blockCached = false;
	
	public HBaseUtil(Configuration conf){			
		try{
			if (conf == null)
				this.conf = HBaseConfiguration.create();
			else
				this.conf = conf;
			
			this.conf.set("hbase.zookeeper.property.clientPort","2181");
			
			this.admin = new HBaseAdmin(this.conf);
		}catch(Exception e){
			e.printStackTrace();
			log.info(e.fillInStackTrace());
		}		
	}

	public Configuration getHBaseConfig() {
		return conf;
	}
	
	public void setScanConfig(int cacheSize,boolean blockCache){
		this.cacheSize = cacheSize;
		this.blockCached = blockCached;
	}

	
	public HTable createTable(String tableName, String[] metrics,int max_version) throws IOException {				
		System.out.println("create table for "+tableName);
		try{
			if (admin.tableExists(tableName)) {
				System.out.println(admin.listTables());
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}			
			HTableDescriptor td = this.createTableDescription(tableName, metrics,max_version);
			System.out.println(tableName + ": <=>table descirption : "+td.toString());
			this.admin.createTable(td);			
		}catch(Exception e){
			e.printStackTrace();
			//log.info(e.fillInStackTrace());			
		}			
		return new HTable(conf, tableName);
	}
	
	public void getTableHandler(String tableName){
		try{
			table = new HTable(conf, tableName);
			table.setAutoFlush(true);	
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void insertRow(String rowKey,String[] families, String[] qualifiers, long ts,String[] values) throws Exception{
		if(table == null)
			throw new Exception("No table handler");
		
		Put put = new Put(rowKey.getBytes());
		for(int i=0;i<families.length;i++){
			if(ts > 0){
				put.add(families[i].getBytes(), qualifiers[i].getBytes(), ts, values[i].getBytes());
			}else{
				put.add(families[i].getBytes(), qualifiers[i].getBytes(),values[i].getBytes());
			}			
		}
		table.put(put);
	}
	
	public void closeTableHandler(){
		try{
			if (table != null) 
				table.close();			
		}catch(Exception e){
			e.printStackTrace();
		}

	}
	
	public HTable updateTable(String tableName,String[] metrics,int max_version)throws IOException{
		//log.info("entry: "+tableName + ":"+metrics);
		try{
			
			HTableDescriptor td = this.createTableDescription(tableName, metrics,max_version);
			this.admin.disableTable(tableName);
			this.admin.modifyTable(tableName.getBytes(), td);
			this.admin.enableTable(tableName);	
			
		}catch(Exception e){
			log.info(e.fillInStackTrace());
			e.printStackTrace();
		}
		//log.info("exit");
		return new HTable(tableName);

	}
	
	public void deleteTable(String tableName)throws IOException{
		//log.info("entry: "+tableName);
		try{			
			if(this.admin.tableExists(tableName)){
				this.admin.disableTable(tableName);
				this.admin.deleteTable(tableName);
			}			
		}catch(Exception e){
			log.equals(e.fillInStackTrace());
			e.printStackTrace();
		}
		//log.info("exit");
	}
		
	
	private synchronized HTableDescriptor createTableDescription(String tableName,String[] metrics,int max_version){
		//log.info("entry: "+tableName + ":"+metrics);
		HTableDescriptor td = new HTableDescriptor(tableName);
		try{
			for (int i = 0; i < metrics.length; i++) {				
				String colName = metrics[i];				
				if (colName==null || colName.length() == 0) {
					log.info("Invalid table schema content, contains empty name column.");
					throw new Exception("Invalid table schema content, contains empty name column.");
				}
				HColumnDescriptor hcd = new HColumnDescriptor(colName);
				hcd.setMaxVersions(max_version);
				td.addFamily(hcd);
			}						
		}catch(Exception e){
			//log.error(e.fillInStackTrace());
			e.printStackTrace();
		}
		
		//log.info("exit");
		return td;				
	}

	public Filter getRowFilter(String compareOp, String regex) throws Exception{
		CompareOp operator = null;
		if (compareOp == null)
			throw new Exception("the compare operation is invalid");
		if(regex == null)
			throw new Exception("The regex is invalid");
		
		try{
			if(compareOp.equals("=")){
				operator = CompareFilter.CompareOp.EQUAL;
			}else if(compareOp.equals(">")){
				operator = CompareFilter.CompareOp.GREATER;
			}else if(compareOp.equals("<")){
				operator = CompareFilter.CompareOp.LESS;
			}else if(compareOp.equals(">=")){
				operator = CompareFilter.CompareOp.GREATER_OR_EQUAL;
			}else if(compareOp.equals("<=")){
				operator = CompareFilter.CompareOp.LESS_OR_EQUAL;
			}else{
				throw new Exception("The compare operation: "+compareOp+" is invalid");
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return new RowFilter(operator,new RegexStringComparator(regex));
	}
	
	public Filter getColumnFilter(String family,String qualifer, String compareOp,String threshold) throws Exception{
		
		CompareOp operator = null;
		if (qualifer == null || family == null)
			throw new Exception("the family:qulifer operation is null");
		if(compareOp == null)
			throw new Exception("The compareOp is null");
		if(threshold == null)
			throw new Exception("The threshold is null");
		
		try{
			if(compareOp.equals("=")){
				operator = CompareFilter.CompareOp.EQUAL;
			}else if(compareOp.equals(">")){
				operator = CompareFilter.CompareOp.GREATER;
			}else if(compareOp.equals("<")){
				operator = CompareFilter.CompareOp.LESS;
			}else if(compareOp.equals(">=")){
				operator = CompareFilter.CompareOp.GREATER_OR_EQUAL;
			}else if(compareOp.equals("<=")){
				operator = CompareFilter.CompareOp.LESS_OR_EQUAL;
			}else{
				throw new Exception("The compare operation: "+compareOp+" is invalid");
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return new SingleColumnValueFilter(family.getBytes(),qualifer.getBytes(),operator, threshold.getBytes());	
				
	}
	// Note: there is a jar file from google: google-collections-0.8.jar need to be imported.
	public Filter getTimeStampFilter(List<Long> timestamps) throws Exception{
				
		if (timestamps == null || timestamps.size()<=0)
			throw new Exception("the timestamps list is null");
		
		return new TimestampsFilter(timestamps);	
				
	}	
	
	
	
	public ResultScanner getResultSet(FilterList filterList,String[] family,String[] columns) throws Exception{
		if(table == null)
			throw new Exception("No table handler");
		if(cacheSize < 0)
			throw new Exception("should set cache size before scanning");
		
		Scan scan = null;
		ResultScanner rscanner = null;
		
		try{
			scan = new Scan();
			scan.setMaxVersions(CosmoConstant.MAX_VERION);
			scan.setCaching(this.cacheSize);
			scan.setCacheBlocks(blockCached);
			scan.setFilter(filterList);	
//			if (timeRange != null){
//				if(timeRange.length==1){
//					scan.setTimeStamp(timeRange[0]);	
//				}else if(timeRange.length == 2){
//					scan.setTimeRange(timeRange[0], timeRange[1]);	
//				}
//								
//			}
				
			if(columns != null){
				for(int i=0;i<columns.length;i++){
					scan.addColumn(family[i].getBytes(),columns[i].getBytes());	
				}	
			}
			
			
			//TODO set filter for scan
			rscanner = this.table.getScanner(scan);
			System.out.println("after get the result scanner...");
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return rscanner;
		
	}
	
	
	
	public void getResult(String tableName) {
		System.out.println("get Result from table name : " + tableName);
		Scan s = new Scan();
		ResultScanner ss = null;		
		s.setMaxVersions(3);
		try {			
			HTable table = new HTable(conf, tableName);
					
			ss = table.getScanner(s);			

			System.out.println("Bixidata table description is : "
					+ table.getTableDescriptor().toString());
			int count = 100;
			for (Result r : ss) {
				//List<KeyValue> kv = r.getColumn(Bytes.toBytes("cf"), Bytes.toBytes("attr"));  // returns all versions of this column
				
				for(KeyValue kv: r.getColumn("qq".getBytes(),"pos_x".getBytes())){
					System.out.println(new String(kv.getFamily()) + "= "+ new String(kv.getValue())+";"+kv.getTimestamp()+";");
				}
					
				//System.out.print("the row is : " + new String(r.getRow())+": {");
				
//				for (KeyValue kv : r.raw()) {					
//					System.out.print(new String(kv.getFamily()) + "= "+ new String(kv.getValue())+";"+kv.getTimestamp()+";");
//					
//				}
				//System.out.println("}");
				count --;
				if(count<0)
					break;
			}			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ss.close();
		}

	}

	public HBaseAdmin getAdmin() {
		return admin;
	}
	

}