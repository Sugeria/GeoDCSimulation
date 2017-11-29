package de.huberlin.wbi.dcs.examples;

import java.util.Random;

import org.apache.commons.collections15.functors.ForClosure;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.distributions.ExponentialDistr;
import org.cloudbus.cloudsim.distributions.GammaDistr;
import org.cloudbus.cloudsim.distributions.LognormalDistr;
import org.cloudbus.cloudsim.distributions.LomaxDistribution;
import org.cloudbus.cloudsim.distributions.ParetoDistr;
import org.cloudbus.cloudsim.distributions.UniformDistr;
import org.cloudbus.cloudsim.distributions.WeibullDistr;
import org.cloudbus.cloudsim.distributions.ZipfDistr;

import de.huberlin.wbi.dcs.distributions.NormalDistribution;
import edu.isi.pegasus.planner.namespace.aggregator.Sum;

public class Parameters {

	public static boolean considerDataLocality = false;
	
	// datacenter params
	// Kb / s
	public static long bwpsPerPe = 256;
	// Kb / s
	public static long iopsPerPe = 20 * 1024;

	public static int nOpteron270 = 200;
	public static int nCusPerCoreOpteron270 = 1;
	public static int nCoresOpteron270 = 4;
	public static int mipsPerCoreOpteron270 = 174;

	public static int nOpteron2218 = 200;
	public static int nCusPerCoreOpteron2218 = 1;
	public static int nCoresOpteron2218 = 4;
	public static int mipsPerCoreOpteron2218 = 247;

	public static int nXeonE5430 = 100;
	public static int nCusPerCoreXeonE5430 = 1;
	public static int nCoresXeonE5430 = 8;
	public static int mipsPerCoreXeonE5430 = 355;

	public static int machineType = 3;

	public enum Experiment {
		MONTAGE_TRACE_1, MONTAGE_TRACE_12, MONTAGE_25, MONTAGE_1000, EPIGENOMICS_997, CYBERSHAKE_1000, ALIGNMENT_TRACE, CUNEIFORM_VARIANT_CALL, HETEROGENEOUS_TEST_WORKFLOW
	}

	public static Experiment experiment = Experiment.MONTAGE_25;

	public static boolean outputDatacenterEvents = true;
	public static boolean outputWorkflowGraph = false;
	public static boolean outputVmPerformanceLogs = false;

	// public enum LogParser

	// experiment parameters
	public enum Scheduler {
		STATIC_ROUND_ROBIN, HEFT, JOB_QUEUE, LATE, C3, C2O
	}

	public static Scheduler scheduler = Scheduler.LATE;
	public static int numberOfRuns = 1;

	public enum Distribution {
		EXPONENTIAL, GAMMA, LOGNORMAL, LOMAX, NORMAL, PARETO, UNIFORM, WEIBULL, ZIPF
	}

	// CPU Heterogeneity
	public static Distribution cpuHeterogeneityDistribution = Distribution.NORMAL;
	public static double cpuHeterogeneityCV = 0.4;
	public static int cpuHeterogeneityAlpha = 0;
	public static double cpuHeterogeneityBeta = 0d;
	public static double cpuHeterogeneityShape = 0d;
	public static double cpuHeterogeneityLocation = 0d;
	public static double cpuHeterogeneityShift = 0d;
	public static double cpuHeterogeneityMin = 0d;
	public static double cpuHeterogeneityMax = 0d;
	public static int cpuHeterogeneityPopulation = 0;

	// IO Heterogeneity
	public static Distribution ioHeterogeneityDistribution = Distribution.NORMAL;
	public static double ioHeterogeneityCV = 0.15;
	public static int ioHeterogeneityAlpha = 0;
	public static double ioHeterogeneityBeta = 0d;
	public static double ioHeterogeneityShape = 0d;
	public static double ioHeterogeneityLocation = 0d;
	public static double ioHeterogeneityShift = 0d;
	public static double ioHeterogeneityMin = 0d;
	public static double ioHeterogeneityMax = 0d;
	public static int ioHeterogeneityPopulation = 0;

	// BW Heterogeneity
	public static Distribution bwHeterogeneityDistribution = Distribution.NORMAL;
	public static double bwHeterogeneityCV = 0.2;
	public static int bwHeterogeneityAlpha = 0;
	public static double bwHeterogeneityBeta = 0d;
	public static double bwHeterogeneityShape = 0d;
	public static double bwHeterogeneityLocation = 0d;
	public static double bwHeterogeneityShift = 0d;
	public static double bwHeterogeneityMin = 0d;
	public static double bwHeterogeneityMax = 0d;
	public static int bwHeterogeneityPopulation = 0;

	// CPU Dynamics
	public static double cpuBaselineChangesPerHour = 0.5;
	public static Distribution cpuDynamicsDistribution = Distribution.NORMAL;
	public static double cpuDynamicsCV = 0.054;
	public static int cpuDynamicsAlpha = 0;
	public static double cpuDynamicsBeta = 0d;
	public static double cpuDynamicsShape = 0d;
	public static double cpuDynamicsLocation = 0d;
	public static double cpuDynamicsShift = 0d;
	public static double cpuDynamicsMin = 0d;
	public static double cpuDynamicsMax = 0d;
	public static int cpuDynamicsPopulation = 0;

	// IO Dynamics
	public static double ioBaselineChangesPerHour = 0.5;
	public static Distribution ioDynamicsDistribution = Distribution.NORMAL;
	public static double ioDynamicsCV = 0.033;
	public static int ioDynamicsAlpha = 0;
	public static double ioDynamicsBeta = 0d;
	public static double ioDynamicsShape = 0d;
	public static double ioDynamicsLocation = 0d;
	public static double ioDynamicsShift = 0d;
	public static double ioDynamicsMin = 0d;
	public static double ioDynamicsMax = 0d;
	public static int ioDynamicsPopulation = 0;

	// BW Dynamics
	public static double bwBaselineChangesPerHour = 0.5;
	public static Distribution bwDynamicsDistribution = Distribution.NORMAL;
	public static double bwDynamicsCV = 0.04;
	public static int bwDynamicsAlpha = 0;
	public static double bwDynamicsBeta = 0d;
	public static double bwDynamicsShape = 0d;
	public static double bwDynamicsLocation = 0d;
	public static double bwDynamicsShift = 0d;
	public static double bwDynamicsMin = 0d;
	public static double bwDynamicsMax = 0d;
	public static int bwDynamicsPopulation = 0;

	// CPU noise
	public static Distribution cpuNoiseDistribution = Distribution.NORMAL;
	public static double cpuNoiseCV = 0.028;
	public static int cpuNoiseAlpha = 0;
	public static double cpuNoiseBeta = 0d;
	public static double cpuNoiseShape = 0d;
	public static double cpuNoiseLocation = 0d;
	public static double cpuNoiseShift = 0d;
	public static double cpuNoiseMin = 0d;
	public static double cpuNoiseMax = 0d;
	public static int cpuNoisePopulation = 0;

	// IO noise
	public static Distribution ioNoiseDistribution = Distribution.NORMAL;
	public static double ioNoiseCV = 0.007;
	public static int ioNoiseAlpha = 0;
	public static double ioNoiseBeta = 0d;
	public static double ioNoiseShape = 0d;
	public static double ioNoiseLocation = 0d;
	public static double ioNoiseShift = 0d;
	public static double ioNoiseMin = 0d;
	public static double ioNoiseMax = 0d;
	public static int ioNoisePopulation = 0;

	// BW noise
	public static Distribution bwNoiseDistribution = Distribution.NORMAL;
	public static double bwNoiseCV = 0.01;
	public static int bwNoiseAlpha = 0;
	public static double bwNoiseBeta = 0d;
	public static double bwNoiseShape = 0d;
	public static double bwNoiseLocation = 0d;
	public static double bwNoiseShift = 0d;
	public static double bwNoiseMin = 0d;
	public static double bwNoiseMax = 0d;
	public static int bwNoisePopulation = 0;

	// straggler parameters
	public static double[] likelihoodOfStragglerOfDC = {0.015,0.015};
	public static double[] stragglerPerformanceCoefficientOfDC = {0.5,0.5};
	
	public double likelihoodOfStraggler;
	public double stragglerPerformanceCoefficient;
	
	public void setLikelihoodOfStraggler(double likelihoodOfStraggler) {
		this.likelihoodOfStraggler = likelihoodOfStraggler;
	}
	
	public double getLikelihoodOfStraggler() {
		return likelihoodOfStraggler;
	}
	
	public void setStragglerPerformanceCoefficient(double stragglerPerformanceCoefficient) {
		this.stragglerPerformanceCoefficient = stragglerPerformanceCoefficient;
	}
	
	public double getStragglerPerformanceCoefficient() {
		return stragglerPerformanceCoefficient;
	}
	
	
	
	
	// datacenter number
	public static int numberOfDC = 2;
	
	
	// number of machineType in each datacenter
	public static int[][] nOpteronOfMachineTypeOfDC = {{200,200,100},{200,200,100}};
	
	
	
	
	// Information of machineType
	
	// datacenter params
	// Kb / s
	public static long[] bwpsPerPeOfMachineType = {256,256,256};
	// Kb / s
	public static long[] iopsPerPeOfMachineType = {20 * 1024, 20 * 1024, 20 * 1024};

	public static int[] nCusPerCoreOpteronOfMachineType = {1,1,1};
	public static int[] nCoresOpteronOfMachineType = {4,4,8};
	public static int[] mipsPerCoreOpteronOfMachineType = {174,247,355};

	
	// performance baseline of datacenter
	public static double[] MIPSbaselineOfDC = getMIPSBaseline();
	public static double[] bwBaselineOfDC = getBwBaseline();
	public static double[] ioBaselineOfDC = getIoBaseline();
	
	
	
	// upperbound of inputdata
	public static int ubOfData = 10;
	
	//iteration_bound
	public static int boundOfIter = 50;
	
	
	// number of vms of datacenter
	public static int[] numberOfVMperDC = {200,200};
	
	// vm params
	
	public static int nVms = sumOfVM(numberOfVMperDC);
	public static int taskSlotsPerVm = 1;

	public static double numberOfCusPerPe = 1;
	public static int numberOfPes = 1;
	public static int ram = (int) (1.7 * 1024);
	
	// uplink of datacenter
	public static double[] uplinkOfDC = {500,500};
	
	// downlink of datacenter
	public static double[] downlinkOfDC = {1000,1000};
	
	// the probability for a task to end in failure instead of success once it's
	// execution time has passed
	public static double[] likelihoodOfFailure = {0.002,0.002};
	public static double[] runtimeFactorInCaseOfFailure = {20d,20d};
	
	// the probability for a datacenter failure
	public static double[] likelihoodOfDCFailure = {0.0001,0.0001};

	// the coefficient of variation for information that is typically not
	// available in real-world scenarios
	// e.g., Task progress scores, HEFT runtime estimates
	public static double distortionCV = 0d;

	public static long seed = 0;
	public static Random numGen = new Random(seed);

	public static ContinuousDistribution getDistribution(
			Distribution distribution, double mean, int alpha, double beta,
			double dev, double shape, double location, double shift,
			double min, double max, int population) {
		ContinuousDistribution dist = null;
		switch (distribution) {
		case EXPONENTIAL:
			dist = new ExponentialDistr(mean);
			break;
		case GAMMA:
			dist = new GammaDistr(numGen, alpha, beta);
			break;
		case LOGNORMAL:
			dist = new LognormalDistr(numGen, mean, dev);
			break;
		case LOMAX:
			dist = new LomaxDistribution(numGen, shape, location, shift);
			break;
		case NORMAL:
			dist = new NormalDistribution(numGen, mean, dev);
			break;
		case PARETO:
			dist = new ParetoDistr(numGen, shape, location);
			break;
		case UNIFORM:
			dist = new UniformDistr(min, max);
			break;
		case WEIBULL:
			dist = new WeibullDistr(numGen, alpha, beta);
			break;
		case ZIPF:
			dist = new ZipfDistr(shape, population);
			break;
		}
		return dist;
	}
	
	
	
	
	private static double[] getIoBaseline() {
		double[] result = new double[numberOfDC];
		
		for (int dcindex = 0; dcindex < numberOfDC; dcindex++) {
			double[] machineDis = getMachineDis(dcindex);
			for (int typeindex = 0; typeindex < machineType; typeindex++) {
				result[typeindex] = machineDis[typeindex]*iopsPerPeOfMachineType[typeindex];
			}
		}
		return result;
	}

	private static double[] getBwBaseline() {
		double[] result = new double[numberOfDC];
		
		for (int dcindex = 0; dcindex < numberOfDC; dcindex++) {
			double[] machineDis = getMachineDis(dcindex);
			for (int typeindex = 0; typeindex < machineType; typeindex++) {
				result[typeindex] = machineDis[typeindex]*bwpsPerPeOfMachineType[typeindex];
			}
		}
		return result;
	}

	public static double[] getMachineDis(int dcindex) {
		double[] result = new double[machineType] ;
		int sum = 0;
		for (int typeindex = 0; typeindex < machineType; typeindex++) {
			sum += nOpteronOfMachineTypeOfDC[dcindex][typeindex];
		}
		for (int typeindex = 0; typeindex < machineType; typeindex++) {
			result[typeindex] = nOpteronOfMachineTypeOfDC[dcindex][typeindex]/sum;
		}
		return result;
	}
	
	
	
	
	public static double[] getMIPSBaseline() {
		double[] result = new double[numberOfDC];
		
		for (int dcindex = 0; dcindex < numberOfDC; dcindex++) {
			double[] machineDis = getMachineDis(dcindex);
			for (int typeindex = 0; typeindex < machineType; typeindex++) {
				result[typeindex] = machineDis[typeindex]*mipsPerCoreOpteronOfMachineType[typeindex];
			}
		}
		return result;
	}

	public static int sumOfVM(int[] vmlist) {
		int sumOfVM = 0;
		for (int index = 0; index<numberOfDC;index++) {
			sumOfVM += numberOfVMperDC[index];
		}
		return sumOfVM;
	}

//	public static void parseParameters(String[] args) {
//
//		for (int i = 0; i < args.length; i++) {
//			if (args[i].compareTo("-" + "outputVmPerformanceLogs") == 0) {
//				outputVmPerformanceLogs = Boolean.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "scheduler") == 0) {
//				scheduler = Scheduler.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "numberOfRuns") == 0) {
//				numberOfRuns = Integer.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "heterogeneityCV") == 0) {
//				cpuHeterogeneityCV = ioHeterogeneityCV = bwHeterogeneityCV = Double
//						.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "cpuHeterogeneityCV") == 0) {
//				cpuHeterogeneityCV = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "ioHeterogeneityCV") == 0) {
//				ioHeterogeneityCV = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "bwHeterogeneityCV") == 0) {
//				bwHeterogeneityCV = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "baselineChangesPerHour") == 0) {
//				cpuBaselineChangesPerHour = ioBaselineChangesPerHour = bwBaselineChangesPerHour = Double
//						.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "baselineCV") == 0) {
//				cpuDynamicsCV = ioDynamicsCV = bwDynamicsCV = Double
//						.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "cpuDynamicsCV") == 0) {
//				cpuDynamicsCV = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "ioDynamicsCV") == 0) {
//				ioDynamicsCV = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "bwDynamicsCV") == 0) {
//				bwDynamicsCV = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "noiseCV") == 0) {
//				cpuNoiseCV = ioNoiseCV = bwNoiseCV = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "cpuNoiseCV") == 0) {
//				cpuNoiseCV = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "ioNoiseCV") == 0) {
//				ioNoiseCV = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "bwNoiseCV") == 0) {
//				bwNoiseCV = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "likelihoodOfStraggler") == 0) {
//				likelihoodOfStraggler = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "stragglerPerformanceCoefficient") == 0) {
//				stragglerPerformanceCoefficient = Double.valueOf(args[++i]);
//			}
//			if (args[i].compareTo("-" + "likelihoodOfFailure") == 0) {
//				for (int dcindex = 0;dcindex < numberOfDC;dcindex++) {
//					likelihoodOfFailure[dcindex] = Double.valueOf(args[++i]);
//				}
//				
//			}
//			if (args[i].compareTo("-" + "runtimeFactorInCaseOfFailure") == 0) {
//				for (int dcindex = 0;dcindex < numberOfDC;dcindex++) {
//					runtimeFactorInCaseOfFailure[dcindex] = Double.valueOf(args[++i]);
//				}
//			}
//		}
//
//	}

}
