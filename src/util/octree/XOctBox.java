package util.octree;


public class XOctBox {
	
    private float top;
    private float bottom;
    private float left;
    private float right;
    private float front;
    private float back;
    
    public XOctBox(float xMin,float xMax,float yMin, float yMax, float zMin,float zMax)
    {
    	left = xMin;
    	right = xMax;
    	bottom = yMin;
        top = yMax;               
        front = zMin;
        back = zMax;

    }
    
    public boolean within(XOctBox Box)
    {
        return within(Box.getLeft(), Box.getRight(), Box.getBottom(), Box.getTop(), Box.getFront(), Box.getBack());
    }
    public boolean within(float xMin, float xMax, float yMin, float yMax, float zMin, float zMax)
    {
        if (xMin >= this.right ||
            xMax < this.left ||
            yMin >= this.top ||
            yMax < this.bottom ||
            zMin >= this.back ||
            zMax < this.front)
            return false;

        return true;
    }

    // the boundary is [), so that means the right value is not counted
    public boolean pointWithinBounds(float x, float y, float z)
    {
        if (x < this.right && 
            x >= this.left &&  
            y < this.top && 
            y >= this.bottom && 
            z < this.back && 
            z >= this.front)
            return true;
        else
            return false;
    }    
    
    
    public double borderDistance(float x, float y, float z)
    {
        double nsdistance;
        double ewdistance;
        double fbdistance;

        if (this.left <= x && x <= this.right)
            ewdistance = 0;
        else
            ewdistance = Math.min((Math.abs(x - this.right)), (Math.abs(x - this.left)));

        if (this.bottom <= y && y <= this.top)
            fbdistance = 0;
        else
            fbdistance = Math.min(Math.abs(y - this.top), Math.abs(y - this.bottom));

        if (this.front <= z && z <= this.back)
            nsdistance = 0;
        else
            nsdistance = Math.min(Math.abs(z - this.back), Math.abs(z - this.front));

        return Math.sqrt(nsdistance * nsdistance +
                         ewdistance * ewdistance +
                         fbdistance * fbdistance);
    }    
    
    
    
    
	public float getTop() {
		return top;
	}

	public void setTop(float top) {
		this.top = top;
	}

	public float getBottom() {
		return bottom;
	}

	public void setBottom(float bottom) {
		this.bottom = bottom;
	}

	public float getLeft() {
		return left;
	}

	public void setLeft(float left) {
		this.left = left;
	}

	public float getRight() {
		return right;
	}

	public void setRight(float right) {
		this.right = right;
	}

	public float getFront() {
		return front;
	}

	public void setFront(float front) {
		this.front = front;
	}

	public float getBack() {
		return back;
	}

	public void setBack(float back) {
		this.back = back;
	}   
   
    public String toString(){
    	String msg = "left=>"+this.left+
    				 ";right=>"+this.right+
    				 ";bottom=>"+this.bottom+
    				 ";top=>"+this.top+
    				 ";front=>"+this.front+
    				 ";bottom=>"+this.bottom;
    	return msg;    				 
    }
    
    public String toSprintf(){
    	String msg = this.left+"\t"+
    				 this.right+"\t"+
    				 this.bottom+"\t"+
    				 this.top+"\t"+
    				 this.front+"\t"+
    				 this.back;
    	return msg;		 
    }
    

}
