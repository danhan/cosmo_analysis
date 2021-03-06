package util;

import cos.dataset.parser.CosmoConstant;

public class Common {
	
	public static boolean doCompare(int type,String source,String compareOp,String threshold){
		//System.out.println(type + ";"+source+";"+compareOp+";"+threshold);
		float diff = -1;
		if(type == CosmoConstant.COSMO_DATA_TYPE_LONG){
			diff = Long.parseLong(source)-Long.parseLong(threshold);
		}else if(type == CosmoConstant.COSMO_DATA_TYPE_FLOAT ){
			diff = (Float.parseFloat(source)-Float.parseFloat(threshold));
		}else{
			throw new RuntimeException("invalid type "+type );				
		}
				
		try{
			if(compareOp.equals("!=")){
				return diff != 0;
			}else if(compareOp.equals("=")){
				return diff == 0;
			}else if(compareOp.equals(">")){				
				return diff > 0;
			}else if(compareOp.equals("<")){
				return diff < 0;
			}else{
				throw new RuntimeException("The compare operation: "+compareOp+" is invalid");
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}		
		return false;	
	}

	public static String reverseIndex(String pId){
		pId = CosmoConstant.IndexFormatter.format(Long.valueOf(pId));		
		StringBuffer sb = new StringBuffer(pId);		
		return sb.reverse().toString();
	}
	
}
