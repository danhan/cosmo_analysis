package util.octree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/*
 * The octree provides the functions:
 * 1 create the tree based on the 3-d data
 * 2 return points based on the given point and distance.
 */
public class XOcTree {
	
	private XOctNode root;
	private long scale = 100000;
	
    public XOcTree(float xMin,float xMax,float yMin,float yMax,float zMin,float zMax,int maxItems,long scale)
    {  
    	this.setScale(scale);
    	xMin = xMin * scale;
    	xMax = xMax * scale+Float.MIN_VALUE;
    	yMin = yMin * scale;
    	yMax = yMax * scale+Float.MIN_VALUE;
    	zMin = zMin * scale;
    	zMax = zMax * scale+Float.MIN_VALUE;
        root = new XOctNode(xMin,xMax,yMin,yMax,zMin,zMax,maxItems,null);        
    }
    
    public boolean insert(float x, float y, float z,long value)
    {
    	x = x * scale;
    	y = y * scale;
    	z = z * scale;
        return root.insert(new XOctPoint(x,y,z,value));
    } 
    
    /*
     * get the points near to the given point
     */
    public ArrayList getNearPoints(float x, float y, float z, double radius)
    {
    	x = x * scale;
    	y = y * scale;
    	z = z * scale;
    	
    	ArrayList nodes = root.getNearPoints(x, y, z, radius);
        return nodes;
    } 
    
    /*
     * get all objects in the tree
     */
    public List<XOctPoint> getAllObjects(List<XOctPoint> points){
    	return root.getAllObjects(points);
    }
    
    /*
     * get all leaf node in the tree
     */    
    public ArrayList<XOctNode> getAllLeafNode(ArrayList<XOctNode> nodes){
    	return root.getAllLeafNode(nodes);
    }
    
    /*
     * point scale
     */
    public void setScale(long scale){
    	this.scale = scale;
    }
    
    
    public int itemCount(){
    	return root.itemCount();
    }

    
    public String getDistanceArea(float x, float y, float z, double radius)
    {
    	x = x * scale;
    	y = y * scale;
    	z = z * scale;
    	
    	return root.getDistanceArea(x, y, z, radius);
        
    } 
    
    public XOctPoint lookup(float x,float y,float z){
    	x = x * scale;
    	y = y * scale;
    	z = z * scale;
    	return root.getNearestPoint(x, y, z, 0);
    }
    
   
}

