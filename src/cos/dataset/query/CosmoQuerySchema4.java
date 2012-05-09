package cos.dataset.query;

import java.io.BufferedReader;

import cos.dataset.parser.CosmoConstant;
import cos.dataset.space.analysis.SpaceQuadTreeIndexing;
import cos.dataset.space.analysis.X3Point;

public class CosmoQuerySchema4 extends CosmoQuerySpace{
	
	/*
	 * 1 Get the neighbours in client side 
	 * 2 send the neighbours particles id and get all information about neighbours
	 * @see cos.dataset.query.CosmoQueryAbstraction#findNeigbour(util.octree.X3DPoint, int, double, long)
	 */
	@Override
	public void findNeigbour(X3Point<Double> p, double distance,long snapshot) {
		// TODO 
		BufferedReader br = null;
		try{
			// createMemoryTree(snapshot)
			SpaceQuadTreeIndexing indexing = new SpaceQuadTreeIndexing(-1, 1, -1, 1,-1, 1,3);
			String indexingFile = CosmoConstant.DATA_INPUT+"/"+snapshot+"-index";
			indexing.buildSnapshotTree(CosmoConstant.DATA_TEST_INPUT,snapshot,indexingFile);
//			float x = p.getX();
//			float y = p.getY();
//			float z = p.getZ();			
//			X3Point lookup = indexing.getTree().lookup(x, y, z);
//			System.out.println("looking up: "+lookup);
//			String boxId = indexing.getTree().getDistanceArea(x,y,z,0.1);
//			System.out.println("the box id: "+boxId);
			 
		}catch(Exception e){
			e.printStackTrace();
		}


	}
}
