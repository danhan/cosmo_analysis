package util;

import java.nio.ByteBuffer;


// from little endian to ASCII
public class DataTransformation {

	
	public static long getLongFromByteArray(byte source[]){		
		byte dest[] = new byte[8];
		if (source.length != 8)			
			return Long.MAX_VALUE;
						
		dest[0] = source[7];
		dest[1] = source[6];
		dest[2] = source[5];
		dest[3] = source[4];		
		dest[4] = source[3];
		dest[5] = source[2];
		dest[6] = source[1];
		dest[7] = source[0];
		
		return ByteBuffer.wrap(dest).getLong();		
		
	}
	
	public static float getFloatFromByteArray(byte source[]){		
		byte dest[] = new byte[4];
		if (source.length != 4)			
			return Long.MAX_VALUE;
						
		dest[0] = source[3];
		dest[1] = source[2];
		dest[2] = source[1];
		dest[3] = source[0];		
		
		return ByteBuffer.wrap(dest).getFloat();				
	}		
	
}
