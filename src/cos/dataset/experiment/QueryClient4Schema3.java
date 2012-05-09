package cos.dataset.experiment;

import cos.dataset.query.CosmoQuerySchema3;
import cos.dataset.query.CosmoQueryTime;

public class QueryClient4Schema3 extends QueryClientBase{

	@Override
	CosmoQueryTime getQueryEngine() {
		return new CosmoQuerySchema3();
	}
	
	public static void main(String[] args) throws Exception{
		QueryClient4Schema3 schema3 = new QueryClient4Schema3();
		if(args.length<3){
			throw new Exception("please input coprocs/not, query/(1,2,3), input-argus");
		}
		int coprocs = Integer.parseInt(args[0]);
		int query = Integer.parseInt(args[1]);
		String property = args[2];
		schema3.execute(coprocs, query, property);		
	}
	
}
