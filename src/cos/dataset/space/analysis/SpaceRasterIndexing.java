package cos.dataset.space.analysis;

import java.text.DecimalFormat;

import util.octree.X3DPoint;

/*
 * store the point into a vector in hbase, 
 * rowkey is the index of row, range [0,x_precise-1] which should be formated.
 * column is the index of column, column is the true value of y location
 * version is the 3rd dimension index, range [0,z_preciese-1] which should be formated as Long
 */
public class SpaceRasterIndexing {

	XCube<Long> space_box = null;
	int x_precise = 10000;	
	int z_precise = 1000;
	long z_stride = 1000;	
	int offset = 1;
	
	DecimalFormat xIndexFormatter = new DecimalFormat("000000000"); 
	long scale = 10000000;
	
	public SpaceRasterIndexing(float xMin,float xMax,float yMin,float yMax,float zMin,float zMax,
						int offset,int x_precise,int z_precise){
		this.offset = offset;
		long x_min = ((long)xMin+offset) * scale;
		long x_max = ((long)xMax+offset) * scale;
		long y_min = ((long)yMin+offset) * scale;
		long y_max = ((long)yMax+offset) * scale;
		long z_min = ((long)zMin+offset) * scale;
		long z_max = ((long)zMax+offset) * scale;
		space_box = new XCube<Long>(x_min,x_max,y_min,y_max,z_min,z_max);
		this.setX_precise(x_precise);		
		this.setZ_precise(z_precise);
		this.setZ_stride();
	}
	
	private X3Point<Long> Normalize(X3Point<Float> point){
		
		long x = (long)((point.getX()+this.offset) * this.scale);
		long y = (long)((point.getY()+this.offset) * this.scale);
		long z = (long)((point.getZ()+this.offset) * this.scale);
		long value = point.getValue(); // the point index
		X3Point<Long> result = new X3Point<Long>(x,y,z,value);		
		return result;
	}
	
	/*
	 * This will 
	 */
	public void createVectorMap(int x_precise,int y_precise,int z_precise){
		
	}
	
	
	/*
	 * This will be called by insert data into database
	 * rowkey, columnID, 3rdId
	 */
	public Object[] getCell4Point(X3Point<Float> point){
		
		System.out.println(point.toString());
		X3Point<Long> normized = this.Normalize(point);
		System.out.println(normized.toString()+"\n");
		
		Object[] cell = new Object[4]; // rowkey, column, version
		
		cell[0] = xIndexFormatter.format(normized.getX() / ((this.scale*10)/this.x_precise)); // string
		
		cell[1] = Long.valueOf(normized.getY()); // String
		
		cell[2] = normized.getZ() / this.z_stride; // long for version number
		
		cell[3] = Long.valueOf(normized.getValue()); // long for particle Index id
		
		return cell;
	}
	
	/*
	 *  find the exterior sphere for the point with the distance R
	 *  given point (x,y,z) and R
	 *  return: exterior square: (x1,x2), (y1,y2),(z1,z2),
	 */
	public X3Point[] getExteriorArea(X3Point point,double r){
		return null;
	}	
	
	
	/*
	 * find the inscribed sphere area for the point with the distance R
	 * given point (x,y,z) and R
	 * return inscribed square: (x1,x2), (y1,y2),(z1,z2),
	 */
	public X3Point[] getInscribedArea(X3Point point,double r){
		return null;
	}
	
	/*
	 * prune the four square aread in four angles between the two squre area 
	 * return 4 rectangeles left which need to be compared 
	 */
	public void pruneEdgeArea(X3Point[] exterior, X3Point[] inscribed){
		
	}

	public XCube<Long> getSpace_box() {
		return space_box;
	}

	public void setSpace_box(XCube<Long> space_box) {
		this.space_box = space_box;
	}

	public int getX_precise() {
		return x_precise;
	}

	public void setX_precise(int x_precise) {
		if(x_precise>0)
			this.x_precise = x_precise;
	}


	public long getZ_stride() {
		return z_stride;
	}

	public void setZ_stride() {
		if(this.scale > 0 && this.z_precise > 0){
			this.z_stride = (this.scale*10)/ this.z_precise;	// because the scale is one less zero than the number of letter in the formatting string
		}
		
	}

	public int getZ_precise() {
		return z_precise;
	}

	public void setZ_precise(int z_precise) {
		if(z_precise > 0)
			this.z_precise = z_precise;
	}
	
}
