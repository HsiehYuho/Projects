package application;

public class WinningRate {
	public static void main(String[] args) {
		WinningRate a = new WinningRate();
		System.out.println(Math.pow(0.99, 200));
		System.out.print(a.notLostConsecutively(0.8, 82));
	}
	public double notLostConsecutively(double winRate, int numberOfGame) {
		double[] rate = new double[numberOfGame+1]; // not to lose consecutively
		rate[0] = 1;
		rate[1] = winRate;
		for(int i = 2; i <= numberOfGame; i++) {
			rate[i] = rate[i-1]*(winRate) + rate[i-2]*(1-winRate)*winRate;
		}
		return rate[numberOfGame] + rate[numberOfGame-1]*(1-winRate);
		
	}
}
