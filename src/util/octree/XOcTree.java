package util.octree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


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
        return root.insert(new X3DPoint(x,y,z,value));
    } 
    
    public List<X3DPoint> getAllObjects(List<X3DPoint> points){
    	return root.getAllObjects(points);
    }
    
    public ArrayList<XOctNode> getAllLeafNode(ArrayList<XOctNode> nodes){
    	return root.getAllLeafNode(nodes);
    }
    
    
    public void setScale(long scale){
    	this.scale = scale;
    }
    
    
    public int itemCount(){
    	return root.itemCount();
    }
    
    public ArrayList getNearPoints(float x, float y, float z, double radius)
    {
    	x = x * scale;
    	y = y * scale;
    	z = z * scale;
    	ArrayList nodes = new ArrayList();
    	nodes = root.getNearPoints(x, y, z, radius,nodes);
        return nodes;
    } 
    
    public String getDistanceArea(float x, float y, float z, double radius)
    {
    	x = x * scale;
    	y = y * scale;
    	z = z * scale;
    	
    	return root.getDistanceArea(x, y, z, radius);
        
    } 
    
    public X3DPoint lookup(float x,float y,float z){
    	x = x * scale;
    	y = y * scale;
    	z = z * scale;
    	return root.getNearestPoint(x, y, z, 0);
    }
    
    
    public static void main(String args[]){
    	
    	//XOcTree tree = new XOcTree(1,(float)10.1,1,(float)10.1,1,(float)10.1,100,1);
    	XOcTree tree = new XOcTree((float)-1,(float)1,(float)-1,(float)1,-1,1,2,1000000);
    	int count = 0;
    	
//    	// 0.127846;0.320566;0.215989
//    	-0.246109;0.36794;-0.477013
//    	0.298784;0.227101;0.349293
//    	-0.446048;-0.486656;-0.369577
//    	-0.285465;0.18415;-0.202595
//    	-0.318106;0.202912;-0.232909
//    	0.0884433;0.286584;0.27584

    	float x = (float)0.127846;
    	float y = (float)0.320566;
    	float z = (float)0.215989;   	
    	tree.insert(x, y, z, 1);
    	
    	System.out.println("item count is "+tree.itemCount());
    	
    	List<X3DPoint> points = new LinkedList<X3DPoint>();
    	points = tree.getAllObjects(points);
    	
    	System.out.println("size is : "+points.size());
    	for(int i=0;i<points.size();i++){
    		System.out.println("num"+i+"\t"+points.get(i).toString());
    	}
//    	
    	System.out.println("\nget all nodes............");
    	ArrayList<XOctNode> nodes = new ArrayList<XOctNode>();
    	nodes = tree.getAllLeafNode(nodes);
    	System.out.println("size is : "+nodes.size());
    	int point_num = 0;
    	for(int i=0;i<nodes.size();i++){    		
    		point_num += nodes.get(i).getPointSize();
    		System.out.println(nodes.get(i).toSprintf());
    	}   	
    	
    	System.out.println("points are "+point_num);
    	System.out.println("size is : "+points.size());
//    	
//    	X3DPoint point = tree.lookup(9,9,1);
//    	//System.out.println(point.size());
//    	System.out.println(point.toString());
    	
//    	ArrayList nodes = tree.getNearPoints(5, 5, 5, 1);
//    	System.out.println("nearest points : "+nodes.size());
//    	
//    	for(int i=0;i<nodes.size();i++){
//    		System.out.println(nodes.get(i).toString());
//    	}
    	

    	   	
    }
    
}
