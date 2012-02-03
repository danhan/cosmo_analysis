package cos.dataset.parser;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import util.DataTransformation;

public class CosmologyParser {
	

	public static void main(String args[]){
				
		String fileDir = args[0];
		
		File dir = new File(fileDir);
		if (!dir.isDirectory()) {
			System.out.println(" dir is: " + dir.getAbsolutePath());
			System.exit(1);
		}
		
		String[] fileNames = dir.list();
		CosmologyParser parser = new  CosmologyParser();
		
		long start = System.currentTimeMillis();
		for (String fileName : fileNames) { 
			if (!fileName.endsWith(".bin")) continue;
			fileName = fileDir + "/"+fileName;	
			parser.parse(fileName, fileName.substring(0,fileName.lastIndexOf('.'))+".out");
		}
		
		System.out.println("execution time: "+ (System.currentTimeMillis()-start));		
	}
		
	
	
	public void parse(String inFile, String outFile) {
		if (inFile != null) {
			try {
				File file = new File(inFile);
				if (file.exists()) {
					if (inFile.contains("gas")) {
						common_parser(inFile, outFile, CosmoConstant.ENUM_GAS.toHeader(),
								CosmoConstant.ENUM_GAS.values().length);
					} else if (inFile.contains("dark")) {
						common_parser(inFile, outFile,
								CosmoConstant.ENUM_DARK_MATTER.toHeader(),
								CosmoConstant.ENUM_DARK_MATTER.values().length);
					} else if (inFile.contains("star")) {
						common_parser(inFile, outFile, CosmoConstant.ENUM_STAR.toHeader(),
								CosmoConstant.ENUM_STAR.values().length);
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
	
	

	private void common_parser(String inFile, String outFile, String header,
			int num) {
		DataInputStream input = null;
		OutputStreamWriter output = null;
		try {
			input = new DataInputStream(new FileInputStream(inFile));
			if (outFile != null) {
				output = new OutputStreamWriter(new FileOutputStream(outFile));
			}
			byte longValue[] = new byte[8];
			byte floatValue[] = new byte[4];
			int line_num = 0;
			if (null != output)
				output.write("num\t" + header);

			// iOrder, mass, position x, y, z, velocity x, y, z, phi, metals,
			// tform, eps
			while (true) {
				int count = input.read(longValue);
				if (count <= 0)
					break;

				line_num++;
				long iOrder = DataTransformation
						.getLongFromByteArray(longValue);
				output.write(line_num + "\t" + iOrder);

				for (int i = 0; i < num-1; i++) {
					input.read(floatValue);
					float metrics = DataTransformation
							.getFloatFromByteArray(floatValue);					
					output.write("\t" + metrics);
				}
				output.write("\n");				

			}

			System.out.println("num of lines : " + line_num);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != input)
					input.close();
				if (null != output)
					output.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}


}
