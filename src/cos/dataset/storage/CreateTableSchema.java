package cos.dataset.storage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import cos.dataset.parser.CosmoConstant;

import util.hbase.HBaseUtil;

public class CreateTableSchema {
	static private Configuration conf = HBaseConfiguration.create();
	
	
	public static void main(String[] args) throws IOException {

		if(args.length < 1){
			return;
		}		
		HBaseUtil hbaseUtil = null;
		try {
			hbaseUtil = new HBaseUtil(conf);
			
			if(args[0].equals("1")){
				String families[] = {CosmoConstant.FAMILY_NAME.toString()};
				hbaseUtil.createTable(CosmoConstant.TABLE_NAME, families,CosmoConstant.MAX_VERION);
				System.out.println("finish creating the table: " + CosmoConstant.TABLE_NAME);				
			}else if(args[0].equals("2")){
				String families[] = {CosmoConstant.FAMILY_NAME.toString()};
				hbaseUtil.createTable(CosmoConstant.TABLE_NAME_2, families,1);
				System.out.println("finish creating the table: " + CosmoConstant.TABLE_NAME_2);
			}else if(args[0].equals("3")){
				String families[] = {CosmoConstant.FAMILY_NAME.toString()};
				hbaseUtil.createTable(CosmoConstant.TABLE_NAME_3, families,CosmoConstant.MAX_VERION);
				System.out.println("finish creating the table: " + CosmoConstant.TABLE_NAME_3);					
			}else if(args[0].equals("4")){
				String families[] = {CosmoConstant.FAMILY_NAME.toString()};
				hbaseUtil.createTable(CosmoConstant.TABLE_NAME_4, families,1);
				System.out.println("finish creating the table: " + CosmoConstant.TABLE_NAME_4);				
			}else if(args[0].equals("5")){
				String families[] = {CosmoConstant.FAMILY_NAME.toString()};
				hbaseUtil.createTable(CosmoConstant.TABLE_NAME_5, families,CosmoConstant.X_PRECISION);
				System.out.println("finish creating the table: " + CosmoConstant.TABLE_NAME_5);					
			}else{
				String families[] = new String[]{"t"};
				hbaseUtil.createTable("test", families, CosmoConstant.MAX_VERION);
				System.out.println("finish creating the table: " + "test");
			}
		} catch (Exception e) {
			e.printStackTrace();	
		}
	}
}
