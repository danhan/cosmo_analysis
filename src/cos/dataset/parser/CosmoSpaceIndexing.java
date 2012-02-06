package cos.dataset.parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;

import util.octree.XOcTree;
import util.octree.XOctNode;

public class CosmoSpaceIndexing {

	String inputDir = null;
	String outFile = null;
	XOcTree tree = null;

	public CosmoSpaceIndexing(float xMin, float xMax, float yMin, float yMax,
			float zMin, float zMax, int max_item_per_node) throws Exception {

		this.tree = new XOcTree(xMin, xMax, yMin, yMax, zMin, zMax,
				max_item_per_node);
	}

	public void execute(String inputDir, String outFile) {
		try {
			List<String> fileNameList = this.filterFile(inputDir);
			this.outFile = outFile;
			long s_time = System.currentTimeMillis();
			int count = 0;
			for (String fileName : fileNameList) {
				BufferedReader input = null;
				try {
					input = new BufferedReader(new FileReader(fileName));
					input.readLine();// skip the header
					String line = input.readLine();
					while (line != null) {						
						String[] items = line.split("\t");
						float x = Float
								.parseFloat(items[CosmoConstant.INDEX_POS_X]);
						float y = Float
								.parseFloat(items[CosmoConstant.INDEX_POS_Y]);
						float z = Float
								.parseFloat(items[CosmoConstant.INDEX_POS_Z]);
						long pid = Long
								.parseLong(items[CosmoConstant.INDEX_PID]);						
						count++;
						this.tree.insert(x, y, z, pid);
						
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

	private List filterFile(String fileDir) {
		List<String> fileList = null;
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
				fileList = new LinkedList<String>();
				String path = directory.getAbsolutePath();
				for (String fileName : directory.list()) {
					if (fileName.endsWith(".out")) {
						fileList.add(path + "/" + fileName);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileList;
	}

	private void persistentSpaceIndexing() {		
		if(null != this.outFile){
			BufferedWriter output = null;
			try {
				output = new BufferedWriter(new FileWriter(this.outFile));
				List<XOctNode> nodes = new LinkedList<XOctNode>();
				nodes = tree.getAllLeafNode(nodes);
				for (int i = 0; i < nodes.size(); i++) {
					XOctNode node = nodes.get(i);
					output.write(node.toSprintf() + "\n");
				}
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

	public static void main(String[] args) {
		
		try {
			if(args.length<1){
				throw new Exception("Please input correct arguments");
			}
			String inputDir = args[0];//"./data/first/";
			String outFile = null;
			if (args.length==2)
				outFile = args[1];//"./data/space-indexing";
			
			CosmoSpaceIndexing indexing = new CosmoSpaceIndexing(-1, 1, -1, 1,-1, 1, 1000);
			indexing.execute(inputDir, null);
			
			// look up one point
			long s_time = System.currentTimeMillis();
			float x = (float)-0.38065;
			float y = (float)0.122575;
			float z = (float)-0.00233783;
			System.out.println(indexing.getTree().lookup(x,y,z));
			System.out.println("looking up exe_time=>"+(System.currentTimeMillis()-s_time));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
