package cos.dataset.space.analysis;

public class XCube<T> {

    private T top;
    private T bottom;
    private T left;
    private T right;
    private T front;
    private T back;
    
    public XCube(T xMin,T xMax,T yMin, T yMax, T zMin,T zMax)
    {
    	left = xMin;
    	right = xMax;
    	bottom = yMin;
        top = yMax;               
        front = zMin;
        back = zMax;

    }

	public T getTop() {
		return top;
	}

	public void setTop(T top) {
		this.top = top;
	}

	public T getBottom() {
		return bottom;
	}

	public void setBottom(T bottom) {
		this.bottom = bottom;
	}

	public T getLeft() {
		return left;
	}

	public void setLeft(T left) {
		this.left = left;
	}

	public T getRight() {
		return right;
	}

	public void setRight(T right) {
		this.right = right;
	}

	public T getFront() {
		return front;
	}

	public void setFront(T front) {
		this.front = front;
	}

	public T getBack() {
		return back;
	}

	public void setBack(T back) {
		this.back = back;
	}   
   
    public String toString(){
    	String msg = "left=>"+this.left+
    				 ";right=>"+this.right+
    				 ";bottom=>"+this.bottom+
    				 ";top=>"+this.top+
    				 ";front=>"+this.front+
    				 ";back=>"+this.back;
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
