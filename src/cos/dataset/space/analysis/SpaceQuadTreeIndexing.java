package cos.dataset.space.analysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import cos.dataset.parser.CosmoConstant;

import util.octree.XOctPoint;
import util.octree.XOcTree;
import util.octree.XOctNode;

public class SpaceQuadTreeIndexing {

	String inputDir = null;
	String outFile = null;
	XOcTree tree = null;
	
	public SpaceQuadTreeIndexing(float xMin, float xMax, float yMin, float yMax,
			float zMin, float zMax, int max_item_per_node) throws Exception {
		
		this.tree = new XOcTree(xMin, xMax, yMin, yMax, zMin, zMax,
				max_item_per_node,CosmoConstant.COSMO_DATA_SCALE);
		
	}

	public void buildSnapshotTree(String inputDir, long snapshot, String outFile) {
		try {
			ArrayList<String> fileNameList = this.getSnapshotFamily(inputDir,this.filterFile(inputDir),snapshot);
			this.outFile = outFile;
			long s_time = System.currentTimeMillis();
			int count = 0;
			int failed = 0;
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
						count++;
						if(!this.tree.insert(x, y, z, pid)){
							System.out.println(CosmoConstant.INDEX_NO+"=> failed: "+x+";"+y+";"+z+"; "+pid);
							failed++;
						}
						line = input.readLine();
					}
					if (line == null) {
						System.out.println("finish indexing file: " + fileName+"failed: "+failed);						
						input.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (null != input)
						input.close();
				}

			}
			long i_time = System.currentTimeMillis();
			/***** after indexing step, store them into the file *************/
			this.persistentSpaceIndexing();
			
			long e_time = System.currentTimeMillis();
			System.out.println("count=>"+count+";exe_time=>"+(e_time-s_time)+";index_time=>"+(e_time-i_time));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	

	public XOcTree getTree() {
		return tree;
	}
		

	private ArrayList filterFile(String fileDir) {
		System.out.println(fileDir);
		ArrayList<String> fileList = null;
		try {
			if (null != fileDir) {
				File file = new File(fileDir);
				if (file.isDirectory())
					this.inputDir = fileDir;
				else
					throw new Exception("the file directory is invalid");
			}
			// open file
			File directory = new File(this.inputDir);
			if (directory.isDirectory()) {
				fileList = new ArrayList<String>();							
				for (String fileName : directory.list()) {
					if (fileName.endsWith(".out")) {
						fileList.add(fileName);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
		return fileList;
	}
	
	
	private ArrayList<String> getSnapshotFamily(String inputDir,ArrayList<String> fileList,long snapshot){
    	ArrayList<String> snapshotFiles = new ArrayList<String>();
    	// get three files for the snapshot    	
    	for(String fileName: fileList){
    		if(fileName.split(CosmoConstant.FILE_NAME_DELIMITER)[2].contains(String.valueOf(snapshot))){
    			snapshotFiles.add(inputDir+"/"+fileName);
    		}
    	}
    	return snapshotFiles;
	}
		
	
	private void persistentSpaceIndexing() {		
		if(null != this.outFile){
			BufferedWriter output = null;
			try {
				output = new BufferedWriter(new FileWriter(this.outFile));
				ArrayList<XOctNode> nodes = new ArrayList<XOctNode>();
				nodes = tree.getAllLeafNode(nodes);
				int total_num = 0;
				for (int i = 0; i < nodes.size(); i++) {
					XOctNode node = nodes.get(i);
					total_num+=nodes.get(i).getPointSize();
					output.write(node.toSprintf() + "\n");
				}
				output.write(total_num+"\n");
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (null != output) {
					try {
						output.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}			
		}
	}

}
