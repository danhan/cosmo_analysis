package util.octree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class XOctNode {

	private ArrayList<X3DPoint> points;
	private XOctNode[] branches;
	private XOctBox boundary;
    private int max_points_per_node = 3;    // how many obejcts in a node;
    private String index = "0"; 
    private XOctNode parent = null;
    
    
    public XOctNode(float xMin,float xMax,float yMin,float yMax,float zMin,float zMax,int maximumItems,XOctNode parent)
    {
    	boundary = new XOctBox(xMin,xMax,yMin,yMax, zMin, zMax);
    	max_points_per_node = maximumItems;        
        points = new ArrayList<X3DPoint>();
        this.parent = parent;     
        branches = null;
    }
    
    public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		if(this.parent != null){
			if(!this.parent.index.equals("0")){				
				this.index = this.parent.index+index;	
			}else{				
				this.index = index;
			}		
		}
	}
	
	public void setBranchIndex(){
		this.branches[0].setIndex("000");
		this.branches[1].setIndex("001");
		this.branches[2].setIndex("010");
		this.branches[3].setIndex("011");
		this.branches[4].setIndex("100");
		this.branches[5].setIndex("101");
		this.branches[6].setIndex("110");
		this.branches[7].setIndex("111");		
	}

	/// <summary> Add a OctreeLeaf into the tree at a location.</summary>
    /// <param name="leaf">object-location composite</param>
    /// <returns> true if the pution worked.</returns>
    public boolean insert(X3DPoint point)
    {   	
    	System.out.println("in insert.....point:.."+point.toString());
    	if ( (branches == null) && this.points.size() < this.max_points_per_node)
        {
    		System.out.println(this.index+" branches is null.......");
    		this.addPoint(point);
    		System.out.println(point.toString());
            return true;
        }
        else
        {
        	
        	if(branches == null){
        		System.out.println("branches is null but points > max.points number: "+this.points.size()); 
        		if(this.points.size() == this.max_points_per_node)
        			this.addPoint(point);        		
        		this.split();
        	}else{
        		System.out.println("branches is not null....");
                XOctNode node = this.getChild(point.getX(),point.getY(),point.getZ());                
                if (node != null){
                	System.out.println("insert the point into node: "+node.index);
                	return node.insert(point);
                }else{
                	System.out.println("cannot find child");
                }
        	}
       }
        return false;
    }    
    
    
    /// <summary> This method splits the node into four branch, and disperses
    /// the items into the branch. The split only happens if the
    /// boundary size of the node is larger than the minimum size (if
    /// we care). The items in this node are cleared after they are put
    /// into the branch.
    /// </summary>
    protected void split()
    {
    	System.out.println("+++++++++++++=in split.....");
        float btHalf = (float)(boundary.getTop() - (boundary.getTop() - boundary.getBottom()) * 0.5); // bottom-top half
        float lrHalf = (float)(boundary.getRight() - (boundary.getRight() - boundary.getLeft()) * 0.5); // left-right half
        float fbHalf = (float)(boundary.getBack() - (boundary.getBack() - boundary.getFront()) * 0.5); // front-back half

        branches = new XOctNode[8];

        //left->right,top->bottom,front->back
        branches[0] = new XOctNode(boundary.getLeft(),lrHalf,btHalf,boundary.getTop(),boundary.getFront(),fbHalf,this.max_points_per_node,this); //left-top-front
        branches[1] = new XOctNode(lrHalf,boundary.getRight(),btHalf,boundary.getTop(),boundary.getFront(),fbHalf,this.max_points_per_node,this); // right-top-front
        branches[2] = new XOctNode(boundary.getLeft(),lrHalf, boundary.getBottom(), btHalf, boundary.getFront(),fbHalf,this.max_points_per_node,this); // left-bottom-front
        branches[3] = new XOctNode(lrHalf,boundary.getRight(), boundary.getBottom(), btHalf,boundary.getFront(),fbHalf,this.max_points_per_node,this); // right-bottom-front

        branches[4] = new XOctNode(boundary.getLeft(),lrHalf,btHalf,boundary.getTop(),fbHalf,boundary.getBack(),this.max_points_per_node,this); //left-back-top
        branches[5] = new XOctNode(lrHalf,boundary.getRight(),btHalf,boundary.getTop(),fbHalf,boundary.getBack(),this.max_points_per_node,this);
        branches[6] = new XOctNode(boundary.getLeft(),lrHalf, boundary.getBottom(), btHalf, fbHalf,boundary.getBack(),this.max_points_per_node,this);
        branches[7] = new XOctNode(lrHalf,boundary.getRight(), boundary.getBottom(), btHalf,fbHalf,boundary.getBack(),this.max_points_per_node,this);
        
        this.setBranchIndex();
        
        
        ArrayList<X3DPoint> temp = (ArrayList<X3DPoint>)points.clone();
        points.clear();
        Iterator<X3DPoint> item = temp.iterator();
        System.out.println("reorgnize the points...........................................");
        while (item.hasNext())
        {
        	X3DPoint current = item.next();        	
            this.insert(current);      	
        }
        System.out.println("finish reorgnizing............................................");
    }    
    
    
    private void addPoint(X3DPoint point)
    {
        if (this.points == null)
            this.points = new ArrayList<X3DPoint>();        
        point.setIndex(this.index);
        this.points.add(point);        
    }    
    
    
    /// <summary> Get the node that covers a certain x/y pair.</summary>
    /// <param name="x">up-down location in Octree Grid (x, y)</param>
    /// <param name="y">left-right location in Octree Grid (y, x)</param>
    /// <returns> node if child covers the point, null if the point is
    /// out of range.</returns>
    protected XOctNode getChild(float x, float y, float z)
    {
    	System.out.println("in getChild..............");
        if (this.boundary.pointWithinBounds(x, y, z))
        {
            if (this.branches != null)
            {
                for (int i = 0; i < branches.length; i++)
                    if (branches[i].boundary.pointWithinBounds(x, y, z))
                        return branches[i].getChild(x, y, z);
            }
            else
                return this;
        }else{
        	System.out.println("point within bouds return false++++++++++");
        }
        return null;
    }
    
    public boolean hasChildren()
    {
        if (branches != null)
            return true;
        else
            return false;
    }   
    
    public int itemCount()
    {
        int count = 0;

        // Add the objects at this level
        if (this.points != null) count += this.points.size();

        // Add the objects that are contained in the children
        if (branches != null)
        {
        	for(int i=0;i<branches.length;i++){
        		count += branches[i].itemCount();
        	}
        }

        return count;
    }     
    
    

    public List<X3DPoint> getAllObjects(List<X3DPoint> results)
    {    	
        // If this Quad has objects, add them
        if (this.points != null){        	        	
        	results.addAll(this.points);        	
        }
                       
        // If we have children, get their objects too
        if (this.branches != null)
        {
        	for(int i=0;i<this.branches.length;i++){
        		this.branches[i].getAllObjects(results);
        	}
        }
        return results;
    }
    
    
}
