package cos.dataset.parser;

import java.text.DecimalFormat;

public class CosmoConstant {
	
	public static DecimalFormat IndexFormatter = new DecimalFormat("00000000");
	public static DecimalFormat snapshotFormatter = new DecimalFormat("00000");

	
	/*
	 * all start and stop index are got from statistics
	 */
	public enum ENUM_GAS {
		iOrder, mass, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z, phi, rho, temp, hsmooth, metals;
		public static String toHeader() {
			return "id\t" + "mass\t" + "px\t" + "py\t" + "pz\t"
					+ "vx\t" + "vy\t" + "vz\t" + "phi\t"
					+ "rho\t" + "temp\t" + "hsmooth\t" + "metals\n";		
		}
		
		public static int count(){
			return 13;		
		}
		public static String toSerialize(){
			return "[id," + "mass," + "px," + "py," + "pz,"
					+ "vx," + "vy," + "vz," + "phi,"
					+ "rho," + "temp," + "hsmooth," + "metals]";			
		}

		public static String getStartIndex(){
			return String.valueOf(IndexFormatter.format(0));
					
		}
		public static String getStopIndex(){
			return String.valueOf(IndexFormatter.format(16777215));
		}

		
	};

	public enum ENUM_DARK_MATTER {
		iOrder, mass, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z, phi, eps;
		public static String toHeader() {
			return "id\t" + "mass\t" + "px\t" + "py\t" + "pz\t"
					+ "vx\t" + "vy\t" + "vz\t" + "phi\t"
					+ "eps\n";
		}
		public static int count(){
			return 10;		
		}	
		public static String toSerialize(){
			return "[id," + "mass," + "px," + "py," + "pz,"
					+ "vx," + "vy," + "vz," + "phi,"+ "eps]";			
		}

		public static String getStartIndex(){
			return String.valueOf(IndexFormatter.format(16777216));
		}
		
		public static String getStopIndex(){
			return String.valueOf(IndexFormatter.format(33554431));
		}

		
	};

	public enum ENUM_STAR {
		iOrder, mass, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z, phi, metals, tform, eps;
		public static String toHeader() {
			return "id\t" + "mass\t" + "px\t" + "py\t" + "pz\t"
					+ "vx\t" + "vy\t" + "vz\t" + "phi\t"
					+ "metals\t"+"tform\t"+"eps\n";
		}
		public static int count(){
			return 12;		
		}
		public static String toSerialize(){
			return "[id," + "mass," + "px," + "py," + "pz,"
					+ "vx," + "vy," + "vz," + "phi,"+ "metals,"
					+"tform,"+"eps]";			
		}
		
		public static String getStartIndex(){
			return String.valueOf(IndexFormatter.format(33554432));
		}
		
		public static String getStopIndex(){
			return String.valueOf(IndexFormatter.format(99999999));
		}
					
	};

	
	public final static String TABLE_NAME = "cosmo50";
	public final static String FAMILY_NAME = "pp";
	public static final String FILE_NAME_DELIMITER = "\\.";
	public static final String METRICS_DELIMETER = "\t";
	
	public final static String TABLE_NAME_2 = "cosmo50.2";
	
	public final static String TABLE_NAME_3 = "cosmo50.3";
	
	public static final int MAX_VERION = 10; 
	
	public static final int INDEX_NO = 0;
	public static final int INDEX_PID = 1;
	public static final int INDEX_POS_X = 3;
	public static final int INDEX_POS_Y = 4;
	public static final int INDEX_POS_Z = 5;
	
	/********For Quad Tree indexing****************/
	public static final String SPACE_INDEXING_FILE_NAME="./data/space-indexing.scale";
	
	/**************************************/
	public static final int COSMO_DATA_TYPE_LONG=0; // iOdrer
	public static final int COSMO_DATA_TYPE_FLOAT=1; // other metrics
	
	public static final int COSMO_DATA_SCALE=1;
	
	public static final String DATA_TEST_INPUT="./data/test";
	public static final String DATA_TRIAL_INPUT="./data/first";
	public static final String DATA_INPUT="./data/";
	
	public final static String TABLE_NAME_4 = "cosmo50.4";
	
	/***********For Raster Indexing******************/
	
	public static final int SPACE_SCALE = 10000000;
	public static final int Z_SCALE = 10000000;
	public static final int LOCATION_OFFSET = 1; // space offset to adjust to 0,0,0
	public final static String TABLE_NAME_5 = "cosmo50.5";
	
	
	
}
