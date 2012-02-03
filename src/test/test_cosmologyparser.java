package test;


import cos.dataset.parser.CosmologyParser;

public class test_cosmologyparser {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		CosmologyParser parser = new CosmologyParser();
		String inFile = "./data/cosmo50cmb.256g2bwK.00024.star.bin";
		String outFile = "./data/00024.star.txt";
		parser.parse(inFile, outFile);
		
	}
	


	
}

