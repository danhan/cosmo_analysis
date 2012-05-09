package cos.dataset.query;

import java.util.ArrayList;
import java.util.HashMap;

import util.octree.X3DPoint;

public abstract class CosmoQueryTime extends CosmoQueryAbstraction{
	
	//Q1 : Return all particles whose property X is above a given threshold at step S1
	public abstract void propertyFilter(String particleType,String family, String proper_name,
			String compareOp, int type,String threshold, long snapshot,
			String[] result_families, String[] result_columns);
	
	// Q4: Return gas particles destroyed between step S1 and S2
	// s1-(intersect(s1,s2))
	public abstract void getUnique(int type, long s1,long s2);
		
	// Q5: Return all particles whose property X changes from S1 to S2
	/*
	 * return hashmap <particle, <snapshot, property_value>>
	 * if the particle is not existing, the hashmap value is null
	 */
	public abstract HashMap<String,HashMap<Long,String>> changeTrend(String particleType, String[] particle, ArrayList time_series,String famliy,String column);
	
	//Q3: Return all particles of type T within distance R of point P whose property X is above a threshold computed at timestep S1
	
	
	/*************************For Coprocessor**************************************************/
	
	public abstract ArrayList<String> copPropertyFilter(final String particleType,final String family,final String proper_name,
			final String compareOp, final int type, final String threshold, long snapshot,
			final String[] result_families, final String[] result_columns);
	
	
	public abstract ArrayList<String> copGetUnique(final int type, final long s1, final long s2);	
	
	/*
	 * return hashmap <particle, <snapshot, property_value>>
	 * if the particle is not existing, the hashmap value is null
	 */
	public abstract HashMap<String, HashMap<Long, String>> copChangeTrend(final String particleType, final String[] particle, final ArrayList time_series,String famliy,String column);
	
}

	