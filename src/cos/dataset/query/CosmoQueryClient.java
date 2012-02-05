package cos.dataset.query;

import cos.dataset.parser.CosmoConstant;

public class CosmoQueryClient {

		public static void main(String args[]){
			CosmoQuerySchema1 schema1 = new CosmoQuerySchema1(1);
			CosmoQuerySchema2 schema2 = new CosmoQuerySchema2(2);
			
			String family = CosmoConstant.FAMILY_NAME;
			String proper_name = "pos_x";
			String compareOp = ">";
			String threshold = "0.000006";
			long snapshot = 24;
			String[] result_columns = new String[]{"pos_x"};
			String[] result_families = new String[]{"pp"};
					
			//schema1.propertyFilter(family,proper_name,compareOp,threshold,snapshot,result_families,result_columns);
			//schema2.propertyFilter(family,proper_name,compareOp,threshold,snapshot,result_families,result_columns);
			
			//schema1.getUnique(2, 60, 24);
			//schema1.getUnique(2, 60, 24, new String[]{"pp"},new String[]{"pos_x"});
			//schema1.getUnique(2, 24, 60);
			//schema2.getUnique(2, 60, 24);
			//schema2.getUnique(2, 24, 60);
			/**************************************************/
			//schema1.propertyFilterCoprocs(family, proper_name, compareOp, threshold, snapshot, null, null);
			schema1.getUniqueCoprocs(2, 10,24);
		}
}
