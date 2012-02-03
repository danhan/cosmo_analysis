package util.quadtree;

import java.awt.Rectangle;


public class XNode {
	
	private Rectangle rect;
	private String index;

	public XNode(int x,int y,int w,int h){
		rect = new Rectangle(x,y,w,h);
	}
	
	public Rectangle getRect() {
		return rect;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}
	
	
	
}
