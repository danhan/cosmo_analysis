package cos.dataset.parser;

public class CosmoConstant {

	public enum ENUM_GAS {
		iOrder, mass, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z, phi, rho, temp, hsmooth, metals;
		public static String toHeader() {
			return "iOrder\t" + "mass\t" + "pos_x\t" + "pos_y\t" + "pos_z\t"
					+ "velocity_x\t" + "velocity_y\t" + "velocity_z\t" + "phi"
					+ "rho\t" + "temp\t" + "hsmooth\t" + "metals\n";
		}
	};

	public enum ENUM_DARK_MATTER {
		iOrder, mass, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z, phi, eps;
		public static String toHeader() {
			return "iOrder\t" + "mass\t" + "pos_x\t" + "pos_y\t" + "pos_z\t"
					+ "velocity_x\t" + "velocity_y\t" + "velocity_z\t" + "phi"
					+ "eps\n";
		}
	};

	public enum ENUM_STAR {
		iOrder, mass, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z, phi, metals, tform, eps;
		public static String toHeader() {
			return "iOrder\t" + "mass\t" + "pos_x\t" + "pos_y\t" + "pos_z\t"
					+ "velocity_x\t" + "velocity_y\t" + "velocity_z\t" + "phi"
					+ "metals\t"+"tform\t"+"eps\n";
		}		
	};

	
	public final static String TABLE_NAME = "cosmo50";
	public final static String FAMILY_NAME = "pp";
	public static final String FILE_NAME_DELIMITER = "\\.";
	public static final String METRICS_DELIMETER = "\t";
	
	public final static String TABLE_NAME_2 = "cosmo50.2";
	
	public static final int MAX_VERION = 1000; 
	
}
