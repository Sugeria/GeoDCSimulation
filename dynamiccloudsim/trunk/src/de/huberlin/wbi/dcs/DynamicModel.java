package de.huberlin.wbi.dcs;

import java.util.Random;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.examples.Parameters.Distribution;

public class DynamicModel {

	private double previousTime;
	private final Random numGen;

	private double miCurrentBaseline;
	private double ioCurrentBaseline;
	private double bwCurrentBaseline;

	public DynamicModel(
			Distribution cpuDynamicsDistribution,
			double cpuDynamicsCV,
			int cpuDynamicsAlpha,
			double cpuDynamicsBeta,
			double cpuDynamicsShape,
			double cpuDynamicsLocation,
			double cpuDynamicsShift,
			double cpuDynamicsMin,
			double cpuDynamicsMax,
			int cpuDynamicsPopulation,
			Distribution ioDynamicsDistribution,
			double ioDynamicsCV,
			int ioDynamicsAlpha,
			double ioDynamicsBeta,
			double ioDynamicsShape,
			double ioDynamicsLocation,
			double ioDynamicsShift,
			double ioDynamicsMin,
			double ioDynamicsMax,
			int ioDynamicsPopulation,
			Distribution bwDynamicsDistribution,
			double bwDynamicsCV,
			int bwDynamicsAlpha,
			double bwDynamicsBeta,
			double bwDynamicsShape,
			double bwDynamicsLocation,
			double bwDynamicsShift,
			double bwDynamicsMin,
			double bwDynamicsMax,
			int bwDynamicsPopulation
			) {
		this.previousTime = 0;
		this.numGen = Parameters.numGen;

		changeMiBaseline(
				cpuDynamicsDistribution,
				cpuDynamicsCV,
				cpuDynamicsAlpha,
				cpuDynamicsBeta,
				cpuDynamicsShape,
				cpuDynamicsLocation,
				cpuDynamicsShift,
				cpuDynamicsMin,
				cpuDynamicsMax,
				cpuDynamicsPopulation
				);
		changeIoBaseline(
				ioDynamicsDistribution,
				ioDynamicsCV,
				ioDynamicsAlpha,
				ioDynamicsBeta,
				ioDynamicsShape,
				ioDynamicsLocation,
				ioDynamicsShift,
				ioDynamicsMin,
				ioDynamicsMax,
				ioDynamicsPopulation
				);
		changeBwBaseline(
				bwDynamicsDistribution,
				bwDynamicsCV,
				bwDynamicsAlpha,
				bwDynamicsBeta,
				bwDynamicsShape,
				bwDynamicsLocation,
				bwDynamicsShift,
				bwDynamicsMin,
				bwDynamicsMax,
				bwDynamicsPopulation
				);
	}

	private void changeMiBaseline(
			Distribution cpuDynamicsDistribution,
			double cpuDynamicsCV,
			int cpuDynamicsAlpha,
			double cpuDynamicsBeta,
			double cpuDynamicsShape,
			double cpuDynamicsLocation,
			double cpuDynamicsShift,
			double cpuDynamicsMin,
			double cpuDynamicsMax,
			int cpuDynamicsPopulation
			) {
		double mean = 1d;
		double dev = cpuDynamicsCV;
		ContinuousDistribution dist = Parameters.getDistribution(cpuDynamicsDistribution, 
				mean, cpuDynamicsAlpha, cpuDynamicsBeta, dev, cpuDynamicsShape, 
				cpuDynamicsLocation, cpuDynamicsShift, cpuDynamicsMin, 
				cpuDynamicsMax, cpuDynamicsPopulation);
		miCurrentBaseline = 0;
		while (miCurrentBaseline <= 0) {
			miCurrentBaseline = dist.sample();
		}
	}

	private void changeIoBaseline(
			Distribution ioDynamicsDistribution,
			double ioDynamicsCV,
			int ioDynamicsAlpha,
			double ioDynamicsBeta,
			double ioDynamicsShape,
			double ioDynamicsLocation,
			double ioDynamicsShift,
			double ioDynamicsMin,
			double ioDynamicsMax,
			int ioDynamicsPopulation
			) {
		double mean = 1d;
		double dev = ioDynamicsCV;
		ContinuousDistribution dist = Parameters.getDistribution(ioDynamicsDistribution, 
				mean, ioDynamicsAlpha, ioDynamicsBeta, dev, ioDynamicsShape, 
				ioDynamicsLocation, ioDynamicsShift, ioDynamicsMin, 
				ioDynamicsMax, ioDynamicsPopulation);
		ioCurrentBaseline = 0;
		while (ioCurrentBaseline <= 0) {
			ioCurrentBaseline = dist.sample();
		}
	}

	private void changeBwBaseline(
			Distribution bwDynamicsDistribution,
			double bwDynamicsCV,
			int bwDynamicsAlpha,
			double bwDynamicsBeta,
			double bwDynamicsShape,
			double bwDynamicsLocation,
			double bwDynamicsShift,
			double bwDynamicsMin,
			double bwDynamicsMax,
			int bwDynamicsPopulation
			) {
		double mean = 1d;
		double dev = bwDynamicsCV;
		ContinuousDistribution dist = Parameters.getDistribution(bwDynamicsDistribution, mean, 
				bwDynamicsAlpha, bwDynamicsBeta, dev, bwDynamicsShape, 
				bwDynamicsLocation, bwDynamicsShift, bwDynamicsMin, 
				bwDynamicsMax, bwDynamicsPopulation);
		bwCurrentBaseline = 0;
		while (bwCurrentBaseline <= 0) {
			bwCurrentBaseline = dist.sample();
		}
	}

	public void updateBaselines(
			double timespan,
			double cpuBaselineChangesPerHour,
			double ioBaselineChangesPerHour,
			double bwBaselineChangesPerHour,
			Distribution cpuDynamicsDistribution,
			double cpuDynamicsCV,
			int cpuDynamicsAlpha,
			double cpuDynamicsBeta,
			double cpuDynamicsShape,
			double cpuDynamicsLocation,
			double cpuDynamicsShift,
			double cpuDynamicsMin,
			double cpuDynamicsMax,
			int cpuDynamicsPopulation,
			Distribution ioDynamicsDistribution,
			double ioDynamicsCV,
			int ioDynamicsAlpha,
			double ioDynamicsBeta,
			double ioDynamicsShape,
			double ioDynamicsLocation,
			double ioDynamicsShift,
			double ioDynamicsMin,
			double ioDynamicsMax,
			int ioDynamicsPopulation,
			Distribution bwDynamicsDistribution,
			double bwDynamicsCV,
			int bwDynamicsAlpha,
			double bwDynamicsBeta,
			double bwDynamicsShape,
			double bwDynamicsLocation,
			double bwDynamicsShift,
			double bwDynamicsMin,
			double bwDynamicsMax,
			int bwDynamicsPopulation
			) {
		double timespanInHours = timespan / (60 * 60);

		// Assuming exponential distribution
		double chanceOfMiChange = 1 - Math.pow(Math.E,
				-(timespanInHours * cpuBaselineChangesPerHour));
		double chanceOfIoChange = 1 - Math.pow(Math.E,
				-(timespanInHours * ioBaselineChangesPerHour));
		double chanceOfBwChange = 1 - Math.pow(Math.E,
				-(timespanInHours * bwBaselineChangesPerHour));

		if (numGen.nextDouble() <= chanceOfMiChange) {
			changeMiBaseline(
					cpuDynamicsDistribution,
					cpuDynamicsCV,
					cpuDynamicsAlpha,
					cpuDynamicsBeta,
					cpuDynamicsShape,
					cpuDynamicsLocation,
					cpuDynamicsShift,
					cpuDynamicsMin,
					cpuDynamicsMax,
					cpuDynamicsPopulation
					);
		}
		if (numGen.nextDouble() <= chanceOfIoChange) {
			changeIoBaseline(
					ioDynamicsDistribution,
					ioDynamicsCV,
					ioDynamicsAlpha,
					ioDynamicsBeta,
					ioDynamicsShape,
					ioDynamicsLocation,
					ioDynamicsShift,
					ioDynamicsMin,
					ioDynamicsMax,
					ioDynamicsPopulation
					);
		}
		if (numGen.nextDouble() <= chanceOfBwChange) {
			changeBwBaseline(
					bwDynamicsDistribution,
					bwDynamicsCV,
					bwDynamicsAlpha,
					bwDynamicsBeta,
					bwDynamicsShape,
					bwDynamicsLocation,
					bwDynamicsShift,
					bwDynamicsMin,
					bwDynamicsMax,
					bwDynamicsPopulation
					);
		}
	}

	public double nextMiCoefficient(
			Distribution cpuNoiseDistribution,
			double cpuNoiseCV,
			int cpuNoiseAlpha,
			double cpuNoiseBeta,
			double cpuNoiseShape,
			double cpuNoiseLocation,
			double cpuNoiseShift,
			double cpuNoiseMin,
			double cpuNoiseMax,
			int cpuNoisePopulation
			) {
		double mean = miCurrentBaseline;
		double dev = cpuNoiseCV
				* miCurrentBaseline;
		ContinuousDistribution dist = Parameters.getDistribution(cpuNoiseDistribution, mean, 
				cpuNoiseAlpha, cpuNoiseBeta, dev, cpuNoiseShape, 
				cpuNoiseLocation, cpuNoiseShift, cpuNoiseMin, 
				cpuNoiseMax, cpuNoisePopulation);
		double nextMiCoefficient = 0;
		while (nextMiCoefficient <= 0) {
			nextMiCoefficient = dist.sample();
		}
		return nextMiCoefficient;
	}

	public double nextIoCoefficient(
			Distribution ioNoiseDistribution,
			double ioNoiseCV,
			int ioNoiseAlpha,
			double ioNoiseBeta,
			double ioNoiseShape,
			double ioNoiseLocation,
			double ioNoiseShift,
			double ioNoiseMin,
			double ioNoiseMax,
			int ioNoisePopulation
			) {
		double mean = ioCurrentBaseline;
		double dev = ioNoiseCV
				* ioCurrentBaseline;
		ContinuousDistribution dist = Parameters.getDistribution(ioNoiseDistribution, mean, 
				ioNoiseAlpha, ioNoiseBeta, dev, ioNoiseShape, 
				ioNoiseLocation, ioNoiseShift, ioNoiseMin, 
				ioNoiseMax, ioNoisePopulation);
		double nextIoCoefficient = 0;
		while (nextIoCoefficient <= 0) {
			nextIoCoefficient = dist.sample();
		}
		return nextIoCoefficient;
	}

	public double nextBwCoefficient(
			Distribution bwNoiseDistribution,
			double bwNoiseCV,
			int bwNoiseAlpha,
			double bwNoiseBeta,
			double bwNoiseShape,
			double bwNoiseLocation,
			double bwNoiseShift,
			double bwNoiseMin,
			double bwNoiseMax,
			int bwNoisePopulation
			) {
		double mean = bwCurrentBaseline;
		double dev = bwNoiseCV
				* bwCurrentBaseline;
		ContinuousDistribution dist = Parameters.getDistribution(bwNoiseDistribution, mean, 
				bwNoiseAlpha, bwNoiseBeta, dev, bwNoiseShape, 
				bwNoiseLocation, bwNoiseShift, bwNoiseMin, 
				bwNoiseMax, bwNoisePopulation);
		double nextBwCoefficient = 0;
		while (nextBwCoefficient <= 0) {
			nextBwCoefficient = dist.sample();
		}
		return nextBwCoefficient;
	}

	public double getPreviousTime() {
		return previousTime;
	}

	public void setPreviousTime(double previousTime) {
		this.previousTime = previousTime;
	}

}
