package cos.dataset.query;


import cos.dataset.space.analysis.X3Point;


public abstract class CosmoQuerySpace  extends CosmoQueryAbstraction{
	
	
	//Q2: Return all particles of type T within distance R of point P,go through all snapshots?
	public abstract void findNeigbour(X3Point<Double> p,double distance,long snapshot);
	
}