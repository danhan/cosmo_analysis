package cos.dataset.space.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.HBaseConfiguration;

import cos.dataset.parser.CosmoConstant;

import util.XLog;
import util.hbase.HBaseUtil;

public class InsertSpacialData {
	
	HBaseUtil hbase = null;
	SpaceRasterIndexing rasterIndexing = null;
	boolean debug = false;
	
	XLog myLog = new XLog();
	
	/**
	 * @throws IOException
	 */
	public InsertSpacialData(int schema) throws IOException {
		hbase = new HBaseUtil(HBaseConfiguration.create());
		if(schema==4){
			hbase.getTableHandler(CosmoConstant.TABLE_NAME_4);	
		}else if(schema == 5){
			hbase.getTableHandler(CosmoConstant.TABLE_NAME_5);			
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		if(args.length<2){
			return;
		}
		int schema = Integer.parseInt(args[0]);
		InsertSpacialData inserter = new InsertSpacialData(schema);		
		String fileDir = args[1];
		long snapshot = Long.valueOf(args[2]);
		inserter.uploadRasterMap(fileDir,snapshot);
	}
	/*
	 * upload the location information for one snapshot
	 * This will discover the snapshot related files from the given file directory 
	 */
	public void uploadRasterMap(String fileDir,long snapshot){
		try{
			ArrayList<String> fileNameList = this.getSnapshotFamily(fileDir,this.filterFile(fileDir),snapshot);			
			long s_time = System.currentTimeMillis();
			
			this.rasterIndexing = new SpaceRasterIndexing(-1,1,-1,1,-1,1,CosmoConstant.LOCATION_OFFSET,
						CosmoConstant.X_PRECISION,CosmoConstant.Z_PRECISION);
			int count = 0;
			for (String fileName : fileNameList) {	
				System.out.println("start to index file: "+fileName);
				BufferedReader input = null;
				try {
					input = new BufferedReader(new FileReader(fileName));
					input.readLine();// skip the header
					String line = input.readLine();
					while (line != null) {						
						String[] items = line.split(CosmoConstant.METRICS_DELIMETER);
						float x = Float
								.parseFloat(items[CosmoConstant.INDEX_POS_X]);
						float y = Float
								.parseFloat(items[CosmoConstant.INDEX_POS_Y]);
						float z = Float
								.parseFloat(items[CosmoConstant.INDEX_POS_Z]);
						long pid = Long
								.parseLong(items[CosmoConstant.INDEX_PID]);						
						
						X3Point<Float> point = new X3Point<Float>(x,y,z,pid);
						Object[] cell = this.rasterIndexing.getCell4Point(point);
						
						System.out.println("r:"+cell[0] + ";f:c:"+cell[1]+":t:"+cell[2]+"value:"+cell[3]);
						
						count++;
						line = input.readLine();
					}
					if (line == null) {
						System.out.println("finish indexing file: " + fileName);						
						input.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (null != input)
						input.close();
				}
			}						
			
			long e_time = System.currentTimeMillis();
			System.out.println("count=>"+count+";exe_time=>"+(e_time-s_time));
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private ArrayList<String> filterFile(String fileDir) {
		System.out.println(fileDir);
		ArrayList<String> fileList = null;
		try {
			if (null != fileDir) {
				File file = new File(fileDir);
				if (file.isDirectory()){
					// open file
					File directory = new File(fileDir);
					if (directory.isDirectory()) {
						fileList = new ArrayList<String>();							
						for (String fileName : directory.list()) {
							if (fileName.endsWith(".out")) {
								fileList.add(fileName);
							}
						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}		
		return fileList;
	}
		
	public ArrayList<String> getSnapshotFamily(String inputDir,ArrayList<String> fileList,long snapshot){
    	ArrayList<String> snapshotFiles = new ArrayList<String>();
    	// get three files for the snapshot    	
    	for(String fileName: fileList){
    		if(fileName.split(CosmoConstant.FILE_NAME_DELIMITER)[2].contains(String.valueOf(snapshot))){
    			snapshotFiles.add(inputDir+"/"+fileName);
    		}
    	}
    	return snapshotFiles;
	}
	
}
