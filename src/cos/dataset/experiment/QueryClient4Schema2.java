package cos.dataset.experiment;

import cos.dataset.query.CosmoQueryAbstraction;
import cos.dataset.query.CosmoQuerySchema2;

public class QueryClient4Schema2 extends QueryClientBase{

	@Override
	CosmoQueryAbstraction getQueryEngine() {
		return new CosmoQuerySchema2();
	}

}
