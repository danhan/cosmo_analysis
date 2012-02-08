package cos.dataset.experiment;

import cos.dataset.query.CosmoQueryAbstraction;
import cos.dataset.query.CosmoQuerySchema1;

public class QueryClient4Schema1 extends QueryClientBase{

	@Override
	CosmoQueryAbstraction getQueryEngine() {
		return new CosmoQuerySchema1();
	}
	
	public static void main(String[] args) throws Exception{
		QueryClient4Schema1 schema1 = new QueryClient4Schema1();
		if(args.length<3){
			throw new Exception("please input coprocs/not, query/(1,2), input-argus");
		}
		int coprocs = Integer.parseInt(args[0]);
		int query = Integer.parseInt(args[1]);
		String property = args[2];
		schema1.execute(coprocs, query, property);		
	}
	
}
