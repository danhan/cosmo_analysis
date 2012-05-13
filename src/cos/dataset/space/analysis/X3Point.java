package cos.dataset.space.analysis;

public class X3Point<T> {
	
	T x;
	T y;
	T z;
	String index = "-1";
	long value = -1;
	
	public X3Point(){
		
	}
	
	public X3Point(T x,T y,T z){
		this.x = x;
		this.y = y;
		this.z = z;		
	}
	
	public X3Point(T x,T y,T z,long value){
		this.x = x;
		this.y = y;
		this.z = z;
		this.value = value;
	}

	public T getX() {
		return x;
	}

	public long getValue() {
		return value;
	}

	public void setX(T x) {
		this.x = x;
	}

	public T getY() {
		return y;
	}

	public void setY(T y) {
		this.y = y;
	}

	public T getZ() {
		return z;
	}

	public void setZ(T z) {
		this.z = z;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}
	
	
	public void setValue(long value) {
		this.value = value;
	}

	public boolean isEqual(X3Point<T> point){
		if(this.x == point.x &&
		   this.y == point.y &&
		   this.z == point.z)
			return true;
		return false;
	}
	
	
	public String toString(){
		String msg = "x=>"+this.x+
					";y=>"+this.y+
					";z=>"+this.z+
					";index=>"+this.index+
					";value=>"+this.value;
		return msg;
	}	
}
