package util.octree;

import java.util.LinkedList;
import java.util.List;


public class XOcTree {
	
	private XOctNode root;
	
    public XOcTree(float xMin,float xMax,float yMin,float yMax,float zMin,float zMax,int maxItems)
    {
        root = new XOctNode(xMin,xMax,yMin,yMax,zMin,zMax,maxItems,null);        
    }
    
    public boolean insert(float x, float y, float z)
    {
        return root.insert(new X3DPoint(x,y,z));
    } 
    
    public List<X3DPoint> getAllObjects(List<X3DPoint> points){
    	return root.getAllObjects(points);
    }
    
    public int itemCount(){
    	return root.itemCount();
    }
    
    
    public static void main(String args[]){
    	
    	XOcTree tree = new XOcTree(1,(float)10.1,1,(float)10.1,1,(float)10.1,2);
    	tree.insert(2,2,2);
    	tree.insert(3,2,2);
    	tree.insert(4,2,2);    	
    	tree.insert(5,2,1);    	
    	tree.insert(5,2,2);
    	tree.insert(5,2,3);
    	tree.insert(5,2,4);
    	tree.insert(5,2,5);
    	tree.insert(10,2,10);
    	
    	System.out.println("item count is "+tree.itemCount());
    	
    	List<X3DPoint> points = new LinkedList<X3DPoint>();
    	points = tree.getAllObjects(points);
    	
    	System.out.println("size is : "+points.size());
    	for(int i=0;i<points.size();i++){
    		System.out.println(points.get(i).toString());
    	}
    	
    }
    
}
