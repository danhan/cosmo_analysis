package test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import util.quadtree.XNode;
import util.quadtree.XQuadTree;


public class TestQuadTree {
	

	
	public static void main(String args[]){
		XQuadTree quadTree = null;
		List<XNode> points = null;
		
		int x = 1;
		int y = 1;
		int width = 100;
		int height = 100;
		
		quadTree = new XQuadTree(x,y,width,height,4);
		points = generatedPoint();		
				
		for(XNode point: points)
			quadTree.insert(point);
		
		quadTree.printTree();
		
		System.out.println("total count...."+quadTree.Count());
		System.out.println("count...."+quadTree.TopLeftChild().Count());
		System.out.println("count...."+quadTree.TopRightChild().Count());
		System.out.println("count...."+quadTree.BottomLeftChild().Count());
		System.out.println("count...."+quadTree.BottomRightChild().Count());
				
		// TODO the quad tree cannot go back to the root
		String index = quadTree.match(new XNode(10,10,2,2));
		System.out.println("the index is "+index);	
					
	}	
	

	private static List<XNode> generatedPoint(){
		//Random randomGenerator = new Random();
		
		List<XNode> points = new LinkedList<XNode>();
		int x = 10;
		int y = 10;
		points.add(new XNode(x,y,0,0));
		x = 20;
		y = 20;
		points.add(new XNode(x,y,0,0));
		x = 30;
		y = 30;		
		points.add(new XNode(x,y,1,1));
		x = 40;
		y = 40;
		points.add(new XNode(x,y,1,1));
		x = 50;
		y = 50;
		points.add(new XNode(x,y,1,1));
		x = 60;
		y = 60;
		points.add(new XNode(x,y,1,1));
		x = 25;
		y = 25;
		points.add(new XNode(x,y,0,0));
		x = 50;
		y = 25;
		points.add(new XNode(x,y,0,0));	
		x = 75;
		y = 25;
		points.add(new XNode(x,y,0,0));	
		x = 100;
		y = 25;
		points.add(new XNode(x,y,0,0));	
		x = 100;
		y = 1;
		points.add(new XNode(x,y,0,0));	
		x = 100;
		y = 2;
		points.add(new XNode(x,y,0,0));		
		x = 100;
		y = 3;
		points.add(new XNode(x,y,0,0));		
		
		return points; 				
	}
	
	
	

}
