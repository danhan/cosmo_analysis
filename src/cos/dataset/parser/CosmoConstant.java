package cos.dataset.parser;

public class CosmoConstant {

	public enum ENUM_GAS {
		iOrder, mass, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z, phi, rho, temp, hsmooth, metals;
		public static String toHeader() {
			return "id\t" + "mass\t" + "px\t" + "py\t" + "pz\t"
					+ "vx\t" + "vy\t" + "vz\t" + "phi\t"
					+ "rho\t" + "temp\t" + "hsmooth\t" + "metals\n";
		}
	};

	public enum ENUM_DARK_MATTER {
		iOrder, mass, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z, phi, eps;
		public static String toHeader() {
			return "id\t" + "mass\t" + "px\t" + "py\t" + "pz\t"
					+ "vx\t" + "vy\t" + "vz\t" + "phi\t"
					+ "eps\n";
		}
	};

	public enum ENUM_STAR {
		iOrder, mass, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z, phi, metals, tform, eps;
		public static String toHeader() {
			return "id\t" + "mass\t" + "px\t" + "py\t" + "pz\t"
					+ "vx\t" + "vy\t" + "vz\t" + "phi\t"
					+ "metals\t"+"tform\t"+"eps\n";
		}		
	};

	
	public final static String TABLE_NAME = "cosmo50";
	public final static String FAMILY_NAME = "pp";
	public static final String FILE_NAME_DELIMITER = "\\.";
	public static final String METRICS_DELIMETER = "\t";
	
	public final static String TABLE_NAME_2 = "cosmo50.2";
	
	public static final int MAX_VERION = 15; 
	
	public static final int INDEX_NO = 0;
	public static final int INDEX_PID = 1;
	public static final int INDEX_POS_X = 3;
	public static final int INDEX_POS_Y = 4;
	public static final int INDEX_POS_Z = 5;
	
	/********For indexing****************/
	public static final String SPACE_INDEXING_FILE_NAME="./data/space-indexing.scale";
	
	/**************************************/
	public static final int COSMO_DATA_TYPE_LONG=0; // iOdrer
	public static final int COSMO_DATA_TYPE_FLOAT=1; // other metrics
	
	public static final int COSMO_DATA_SCALE=10000000;
	
}
