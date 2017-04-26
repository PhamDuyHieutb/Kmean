package kmean;

import java.util.ArrayList;



public class Point {

	ArrayList<Point> points = new ArrayList<Point>();
		double x;
		double y;
		
		

		public Point(double x, double y) {
			this.x = x;
			this.y = y;
		}
		public Point(String s){
			String[] a= s.split(" ");
			this.x= Double.parseDouble(a[0]);
			this.y= Double.parseDouble(a[1]);
		}

		public double getX() {
			return x;
		}

		public void setX(double x) {
			this.x = x;
		}

		public double getY() {
			return y;
		}

		public void setY(double y) {
			this.y = y;
		}

		public double dictanceToCent(Point p){
			return Math.sqrt(Math.pow(this.getX()-p.getX(),2) +Math.pow(this.getY()-p.getY(),2)); 
		}
		
		public String toString(){
			return Double.toString(x)+" "+Double.toString(y);
		}
	
	}
