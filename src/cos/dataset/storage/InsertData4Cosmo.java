package cos.dataset.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import util.hbase.HBaseUtil;
import util.octree.XOcTree;
import cos.dataset.parser.CosmoConstant;
import cos.dataset.parser.CosmoSpaceIndexing;

public class InsertData4Cosmo {

	HBaseUtil hbase = null;
	XOcTree tree = null;
	
	/**
	 * @throws IOException
	 */
	public InsertData4Cosmo(int schema) throws IOException {
		hbase = new HBaseUtil(HBaseConfiguration.create());
		if(schema==1){
			hbase.getTableHandler(CosmoConstant.TABLE_NAME);	
		}else if(schema == 2){
			hbase.getTableHandler(CosmoConstant.TABLE_NAME_2);
		}				
	}

	public static void main(String[] args) throws IOException {
		if(args.length<2){
			return;
		}
		int schema = Integer.parseInt(args[0]);
		InsertData4Cosmo inserter = new InsertData4Cosmo(schema);		
		String fileDir = args[1];
		
		//create space indexing tree
		try{
			CosmoSpaceIndexing indexing = new CosmoSpaceIndexing(-1, 1, -1, 1,-1, 1, 1000);
			indexing.execute(fileDir, fileDir+"/"+CosmoConstant.SPACE_INDEXING_FILE_NAME);
			inserter.setIndexingTree(indexing.getTree());
		}catch(Exception e){
			e.printStackTrace();
		}
		
		//
		inserter.upload(schema,fileDir);
		
	}
	
	public void setIndexingTree(XOcTree tree){
		this.tree = tree;
	}
	
	private void upload(int schema,String fileDir) {

		File dir = new File(fileDir);
		if (!dir.isDirectory()) {
			System.out.println(" dir is: " + dir.getAbsolutePath());
			System.exit(1);
		}
		String[] fileNames = dir.list();

		long start = System.currentTimeMillis();

		int num_of_particle = 0;
		for (String fileName : fileNames) {
			if (!fileName.endsWith(".out"))
				continue;
			System.out.println("file name : "+fileName);
			
			long snapshot = Long.parseLong(fileName.split(CosmoConstant.FILE_NAME_DELIMITER)[2]);
			fileName = fileDir + "/" + fileName;
		
			int type = 0;
			if (fileName.contains("gas")) {				
				type = 0;
			} else if (fileName.contains("dark")) {
				type = 1;
			} else if (fileName.contains("star")) {
				type = 2;
			}			
			
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(fileName));				
				String header = in.readLine(); // skip the header	
				String[] columns = header.split(CosmoConstant.METRICS_DELIMETER);
				String[] qualifers = new String[columns.length-2];
				String[] families = new String[columns.length-2];
				//TODO if the output file is changed, there should be changed as well.
				for(int i=2;i<columns.length;i++){
					qualifers[i-2] = columns[i];
					families[i-2] = CosmoConstant.FAMILY_NAME;
				}
									 
				String line = in.readLine();
				while(line != null){
					String[] metrics = line.split(CosmoConstant.METRICS_DELIMETER);
					String[] values = new String[metrics.length-1];					
					for(int i=2;i<metrics.length;i++){
						values[i-2] = metrics[i];						
					}
					// TODO get index from the tree
					float x = Float.parseFloat(metrics[CosmoConstant.INDEX_POS_X]);
					float y = Float.parseFloat(metrics[CosmoConstant.INDEX_POS_Y]);
					float z = Float.parseFloat(metrics[CosmoConstant.INDEX_POS_Z]);
					String index =this.tree.lookup(x, y, z).getIndex();					
										
					if(schema==1){
						String rowKey = index+"-"+type+"-"+metrics[CosmoConstant.INDEX_PID];						
						hbase.insertRow(rowKey, families, qualifers, snapshot, values);
					}else if(schema==2){
						String rowKey = snapshot+"-"+index+"-"+type+"-"+metrics[CosmoConstant.INDEX_PID];						
						hbase.insertRow(rowKey, families, qualifers, -1, values);
					}
					num_of_particle++;
					line = in.readLine();
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if (in != null)
						in.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}

		System.out.println("execution time: "+ (System.currentTimeMillis() - start)+";total_number:"+num_of_particle);
	}

}
