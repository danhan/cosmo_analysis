package cos.dataset.space.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;

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
		X3Point<Double> point = new X3Point<Double>(0.5779609,0.5161069,0.14541020);
		XCube cube = inserter.rasterIndexing.getExteriorArea(point, 0.4);
		inserter.rasterIndexing.getRange(cube);		
		// query based on the cube, count the particle returned
		//TODO finished the query
		
		
		
		// query based on octree, count the particle returned
		
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
						1,1,CosmoConstant.SPACE_SCALE);
			int count = 0;
			ArrayList<Put> putList = new ArrayList<Put>();
			int batchRow = 1000;
			int totalRow = 0;
			for (String fileName : fileNameList) {	
				System.out.println("start to index file: "+fileName);
				BufferedReader input = null;
				try {
					input = new BufferedReader(new FileReader(fileName));
					input.readLine();// skip the header
					String line = input.readLine();
					
					while (line != null) {						
						String[] items = line.split(CosmoConstant.METRICS_DELIMETER);
						double x = Double
								.parseDouble(items[CosmoConstant.INDEX_POS_X]);
						double y = Double
								.parseDouble(items[CosmoConstant.INDEX_POS_Y]);
						double z = Double
								.parseDouble(items[CosmoConstant.INDEX_POS_Z]);
						long pid = Long
								.parseLong(items[CosmoConstant.INDEX_PID]);						
						
						X3Point<Double> point = new X3Point<Double>(x,y,z,pid);
						X3Point<Long> cell = this.rasterIndexing.getCell4Point(point);
						
						//System.out.println("r:"+cell.getX() + ";f:c:"+cell.getY()+":t:"+cell.getZ()+"value:"+cell.getValue());
						Put put = hbase.constructRow(String.valueOf(cell.getX()), 
								new String[]{CosmoConstant.FAMILY_NAME}, 
								new String[]{String.valueOf(cell.getY())}, 
								cell.getZ(), 
								new String[]{String.valueOf(cell.getValue())});
						putList.add(put);
						
						if(putList.size() == 1000){							
							hbase.flushBufferedRow(putList);
							totalRow += batchRow;
							myLog.debug(totalRow+" rows has been uploaded for file"+fileName);
							putList.clear();
						}						
						
						count++;
						line = input.readLine();
					}
					
					// for the last lines
					if(putList.size()>0){
						hbase.flushBufferedRow(putList);
						totalRow += putList.size();					
						putList.clear();					
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
		}finally{
			this.hbase.closeTableHandler();
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
