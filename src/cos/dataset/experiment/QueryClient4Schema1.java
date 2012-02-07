package cos.dataset.experiment;

import cos.dataset.query.CosmoQueryAbstraction;
import cos.dataset.query.CosmoQuerySchema1;

public class QueryClient4Schema1 extends QueryClientBase{

	@Override
	CosmoQueryAbstraction getQueryEngine() {
		return new CosmoQuerySchema1();
	}
	
	public static void main(String[] args){
		QueryClient4Schema1 schema1 = new QueryClient4Schema1();
		
		
	}
	
}
