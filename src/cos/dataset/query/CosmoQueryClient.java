package cos.dataset.query;

import cos.dataset.parser.CosmoConstant;

public class CosmoQueryClient {

		public static void main(String args[]){
			CosmoQuerySchema1 schema1 = new CosmoQuerySchema1(1);
			
			
			String family = CosmoConstant.FAMILY_NAME;
			String proper_name = "pos_x";
			String compareOp = ">";
			String threshold = "0.000006";
			long snapshot = 60;
			String[] result_columns = new String[]{"pos_x"};
			String[] result_families = new String[]{"pp"};
					
			schema1.propertyFilter(family,proper_name,compareOp,threshold,snapshot,result_families,result_columns);
		}
}
