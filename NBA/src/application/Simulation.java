package application;


public class Simulation {
	public static void main(String[] args) {
		int numOfSimulateTime = 1000;
		int numberOfGame = 82;
		double winningRate = 0.8;
		int countOfValidGame = 0;
		int numberOfSet = 100;
		double record[] = new double[numberOfSet];
		for(int num = 0; num < record.length; num++) {
			countOfValidGame = 0;
			for(int i = 0; i < numOfSimulateTime; i++) {
				if(simulate(numberOfGame, winningRate))
					countOfValidGame++;
			}
			record[num] = (double)countOfValidGame/numOfSimulateTime;
		}
		for(double d : record) {
			System.out.println(d);
		}
	}			
	public static boolean simulate(int numberOfGame, double winningRate) {
		boolean consecutive = false;
		for(int i = 0; i < numberOfGame; i++) {
			int winOrLose = (int)(Math.random()*100);
			if(consecutive) {
				if(winOrLose < (100 - (winningRate*100)))
					return false;
				else 
					consecutive = false;
			}
			else {
				if(winOrLose < (100 - (winningRate*100)))
					consecutive = true;
			}
		}
		return true;
	}
}
