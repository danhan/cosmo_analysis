package util.octree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class XOcTree {
	
	private XOctNode root;
	
    public XOcTree(float xMin,float xMax,float yMin,float yMax,float zMin,float zMax,int maxItems)
    {
        root = new XOctNode(xMin,xMax,yMin,yMax,zMin,zMax,maxItems,null);        
    }
    
    public boolean insert(float x, float y, float z,long value)
    {
        return root.insert(new X3DPoint(x,y,z,value));
    } 
    
    public List<X3DPoint> getAllObjects(List<X3DPoint> points){
    	return root.getAllObjects(points);
    }
    
    public List<XOctNode> getAllLeafNode(List<XOctNode> nodes){
    	return root.getAllLeafNode(nodes);
    }
    
    public int itemCount(){
    	return root.itemCount();
    }
    
    public ArrayList getNearPoints(float x, float y, float z, double radius)
    {
    	 ArrayList nodes = new ArrayList();
    	 nodes = root.getNearPoints(x, y, z, radius,nodes);
        return nodes;
    } 
    
    public X3DPoint lookup(float x,float y,float z){
    	return root.getNearestPoint(x, y, z, 0);
    }
    
    
    public static void main(String args[]){
    	
    	XOcTree tree = new XOcTree(1,(float)10.1,1,(float)10.1,1,(float)10.1,8);
    	tree.insert(9,9,1,100);
    	tree.insert(2,2,2,111);
    	tree.insert(3,2,2,112);
    	tree.insert(4,2,2,113);    	
    	tree.insert(5,2,1,114);    	
    	tree.insert(5,2,2,115);
    	tree.insert(5,2,3,116);
    	tree.insert(5,2,4,117);
    	tree.insert(5,2,5,118);
    	tree.insert(10,2,10,119);
    	tree.insert(5,5,6,120);
    	
    	System.out.println("item count is "+tree.itemCount());
    	
    	List<X3DPoint> points = new LinkedList<X3DPoint>();
    	points = tree.getAllObjects(points);
    	
    	System.out.println("size is : "+points.size());
    	for(int i=0;i<points.size();i++){
    		System.out.println(points.get(i).toString());
    	}
    	
    	System.out.println("\nget all nodes............");
    	List<XOctNode> nodes = new LinkedList<XOctNode>();
    	nodes = tree.getAllLeafNode(nodes);
    	System.out.println("size is : "+nodes.size());
    	for(int i=0;i<nodes.size();i++){    		
    		System.out.println(nodes.get(i).toSprintf());
    	}   	
    	
    	X3DPoint point = tree.lookup(9,9,1);
    	//System.out.println(point.size());
    	System.out.println(point.toString());
    	
//    	ArrayList nodes = tree.getNearPoints(5, 5, 5, 1);
//    	System.out.println("nearest points : "+nodes.size());
//    	
//    	for(int i=0;i<nodes.size();i++){
//    		System.out.println(nodes.get(i).toString());
//    	}
    	

    	
    	
    }
    
}
