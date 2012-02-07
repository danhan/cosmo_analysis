package cos.dataset.query;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import cos.dataset.parser.CosmoConstant;

public class CosmoQueryClient {

		public static void main(String args[]){
			CosmoQuerySchema1 schema1 = new CosmoQuerySchema1(1);
			CosmoQuerySchema2 schema2 = new CosmoQuerySchema2(2);
			
			String family = CosmoConstant.FAMILY_NAME;
			String proper_name = "pos_x";
			String compareOp = ">";
			String threshold = "-10";
			long snapshot = 60;
			String[] result_columns = new String[]{"pos_x"};
			String[] result_families = new String[]{"pp"};
			int type = CosmoConstant.COSMO_DATA_TYPE_FLOAT;
					
			//schema1.propertyFilter(family,proper_name,compareOp,type,threshold,snapshot,result_families,result_columns);
			//schema2.propertyFilter(family,proper_name,compareOp,type,threshold,snapshot,result_families,result_columns);
			
			//schema1.getUnique(2, 24, 60);
			//schema1.getUnique(2, 60, 24, new String[]{"pp"},new String[]{"pos_x"});
			//schema1.getUnique(2, 24, 60);
			//schema2.getUnique(2, 60, 24);
			//schema2.getUnique(2, 24, 60);
			/**************************************************/
			//schema1.propertyFilterCoprocs(family, proper_name, compareOp, threshold, snapshot, null, null);
			
			BufferedWriter out = null;
			try{
				out = new BufferedWriter(new FileWriter("./data/test1"));				
				ArrayList<String> s1 = schema1.getUniqueCoprocs(2, 60,24);
				System.out.println("return: "+s1.size());
				for(String pid:s1){			
					out.write(pid.substring(pid.lastIndexOf('-'),pid.length())+"\n");
				}				
			}catch(Exception e){
				e.printStackTrace();				
			}finally{
				try{
					out.close();	
				}catch(Exception e){
					e.printStackTrace();
				}				
			}
			
			try{
				out = new BufferedWriter(new FileWriter("./data/test2"));				
				ArrayList<String> s1 = schema2.getUniqueCoprocs(2, 60,24);
				System.out.println("return: "+s1.size());
				for(String pid:s1){			
					out.write(pid.substring(pid.lastIndexOf('-'),pid.length())+"\n");
				}				
			}catch(Exception e){
				e.printStackTrace();				
			}finally{
				try{
					out.close();	
				}catch(Exception e){
					e.printStackTrace();
				}				
			}	
				
		}
}
