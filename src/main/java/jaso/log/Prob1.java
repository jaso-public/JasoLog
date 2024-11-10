package jaso.log;

public class Prob1 {

	public static void main(String[] args) {
		
		double max = 0;
		
		double dt = .01;
		double dr = .01;
		double dw = .1;

		for(double t=23 ; t<25; t+=dt) {
			
			for(double r=40 ; r<50; r+=dr) {
				
				for(double w=1 ; w<0.1*r; w+=dw) {										
					
					double cost = t * r / w  +  r  + r * w;
					//System.out.println(t+" "+r+" "+" "+w+" "+cost);
					if(cost > 500) continue;
					
					double value = t * r * r;
					if(value > max) {
						System.out.println(t+" "+r+" "+" "+w+" "+cost+"=========================="+max+" "+value);
						
						max = value;
					}
				}
			
			}
			
		}
	}
}
