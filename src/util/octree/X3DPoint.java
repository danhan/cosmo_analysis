package util.octree;

public class X3DPoint {
	
	float x;
	float y;
	float z;
	String index = "-1";
	
	public X3DPoint(float x,float y,float z){
		this.x = x;
		this.y = y;
		this.z = z;
	}

	public float getX() {
		return x;
	}

	public void setX(float x) {
		this.x = x;
	}

	public float getY() {
		return y;
	}

	public void setY(float y) {
		this.y = y;
	}

	public float getZ() {
		return z;
	}

	public void setZ(float z) {
		this.z = z;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}
	
	public String toString(){
		String msg = "x=>"+
					this.x+";"+
					"y=>"+this.y+
					"z=>"+this.z+
					"index=>"+this.index;
		return msg;
	}
	
	
}
