package cos.dataset.space.analysis;

import java.text.DecimalFormat;

import util.octree.XOctPoint;

/*
 * store the point into a vector in hbase, 
 * rowkey is the index of row, range [0,x_partition-1] which should be formated.
 * column is the index of column, column is the true value of y location
 * version is the 3rd dimension index, range [0,z_preciese-1] which should be formated as Long
 */
public class SpaceRasterIndexing {

	XCube<Long> space_box = null;
	//int x_partition = 1;	
	//int z_partition = 1;	
	int offset = 1;
	
	DecimalFormat xIndexFormatter = new DecimalFormat("000000000"); 
	long scale = 1;
	
	public SpaceRasterIndexing(float xMin,float xMax,float yMin,float yMax,float zMin,float zMax,
						int offset,int x_partition,int z_partition,int scale){
		this.offset = offset;
		this.scale = scale;
		long x_min = ((long)xMin+offset) * scale;
		long x_max = ((long)xMax+offset) * scale;
		long y_min = ((long)yMin+offset) * scale;
		long y_max = ((long)yMax+offset) * scale;
		long z_min = ((long)zMin+offset) * scale;
		long z_max = ((long)zMax+offset) * scale;
		space_box = new XCube<Long>(x_min,x_max,y_min,y_max,z_min,z_max);
		//this.setx_partition(x_partition);		
		//this.setz_partition(z_partition);			
	}
	
	/*
	 * Normalize the point to the scaled point
	 */
	private X3Point<Long> Normalize(X3Point<Double> point){
		
		long x = (long)((point.getX()+this.offset) * this.scale);
		long y = (long)((point.getY()+this.offset) * this.scale);
		long z = (long)((point.getZ()+this.offset) * this.scale);
		long value = point.getValue(); // the point index
		X3Point<Long> result = new X3Point<Long>(x,y,z,value);		
		return result;
	}
	
	/*
	 * Normalize the distance to the scaled distance 
	 */
	private long Normalize(double r){
		return (long)(r * this.scale);
	}
	
	/*
	 * This will 
	 */
	public void createVectorMap(int x_partition,int y_precise,int z_partition){
		
	}
	
	
	/*
	 * This will be called by insert data into database
	 * rowkey, columnID, 3rdId
	 */
	public X3Point<Long> getCell4Point(X3Point<Double> point){
		
		X3Point<Long> normized = this.Normalize(point);
		
		
		X3Point<Long> cell = new X3Point<Long>(); 		
		
		String row = xIndexFormatter.format(normized.getX() );
		cell.setX(Long.parseLong(row)); // string
				
		cell.setY(normized.getY()); // for column
		
		cell.setZ(normized.getZ());// long for version number
			
		cell.setValue(Long.valueOf(normized.getValue()));// long for particle Index id
	
		//System.out.println(normized.toString());
		System.out.println(cell.toString()+"\n");
		
		return cell;
	}
	
	/*
	 * get the row, column, and version range for the given box
	 */
	public long[] getRange(XCube<Long> box){
		long[] ranges = new long[6];
		ranges[0] = box.getLeft() ;
		ranges[1] = box.getRight();
		ranges[2] = box.getBottom();
		ranges[3] = box.getTop();
		ranges[4] = box.getFront();
		ranges[5] = box.getBack() ;
		for(int i=0;i<ranges.length;i++){
			System.out.print(ranges[i]+",");	
		}
		System.out.println();
		
		return ranges;
	}
	
	/*
	 *  find the exterior sphere for the point with the distance R
	 *  given point (x,y,z) and R
	 *  return: exterior square: (x1,x2), (y1,y2),(z1,z2),
	 */
	public XCube<Long> getExteriorArea(X3Point point,double r){
		
		X3Point<Long> normalized = this.Normalize(point);
		long d = this.Normalize(r);
		
		
		XCube<Long> cube = new XCube<Long>(normalized.getX()-d, normalized.getX()+d,
								normalized.getY()-d,normalized.getY()+d,
								normalized.getZ()-d,normalized.getZ()+d);
		System.out.println(normalized.toString()+"; distance: "+d);
		System.out.println("normalized cube is : "+cube.toString());
		return cube;
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

//	public int getx_partition() {
//		return x_partition;
//	}
//
//	public void setx_partition(int x_partition) {
//		if(x_partition>0)
//			this.x_partition = x_partition;
//	}
//
//
//	public int getz_partition() {
//		return z_partition;
//	}
//
//	public void setz_partition(int z_partition) {
//		if(z_partition > 0)
//			this.z_partition = z_partition;
//	}
	
}
