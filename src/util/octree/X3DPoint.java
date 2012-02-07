package util.octree;

public class X3DPoint {
	
	float x;
	float y;
	float z;
	String index = "-1";
	long value = -1;
	
	public X3DPoint(float x,float y,float z,long value){
		this.x = x;
		this.y = y;
		this.z = z;
		this.value = value;
	}

	public float getX() {
		return x;
	}

	public long getValue() {
		return value;
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
	
	public boolean isEqual(X3DPoint point){
		if(this.x == point.x &&
		   this.y == point.y &&
		   this.z == point.z)
			return true;
		return false;
	}
	
	public String toString(){
		String msg = "x=>"+this.x+";"+
					";y=>"+this.y+
					";z=>"+this.z+
					";index=>"+this.index+
					";value=>"+this.value;
		return msg;
	}	
}
