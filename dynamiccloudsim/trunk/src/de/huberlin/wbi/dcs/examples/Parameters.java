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

	public int[] nOpteronOfMachineType = {200,200,100};
	
	// CPU Heterogeneity
	public static Distribution[] cpuHeterogeneityDistributionOfDC = {Distribution.NORMAL,Distribution.NORMAL};
	public static double[] cpuHeterogeneityCVOfDC = {0.4,0.4};
	public static int[] cpuHeterogeneityAlphaOfDC = {0,0};
	public static double[] cpuHeterogeneityBetaOfDC = {0d,0d};
	public static double[] cpuHeterogeneityShapeOfDC = {0d,0d};
	public static double[] cpuHeterogeneityLocationOfDC = {0d,0d};
	public static double[] cpuHeterogeneityShiftOfDC = {0d,0d};
	public static double[] cpuHeterogeneityMinOfDC = {0d,0d};
	public static double[] cpuHeterogeneityMaxOfDC = {0d,0d};
	public static int[] cpuHeterogeneityPopulationOfDC = {0,0};;
	
	// CPU Default Heterogeneity
	public Distribution cpuHeterogeneityDistribution = Distribution.NORMAL;
	public double cpuHeterogeneityCV = 0.4;
	public int cpuHeterogeneityAlpha = 0;
	public double cpuHeterogeneityBeta = 0d;
	public double cpuHeterogeneityShape = 0d;
	public double cpuHeterogeneityLocation = 0d;
	public double cpuHeterogeneityShift = 0d;
	public double cpuHeterogeneityMin = 0d;
	public double cpuHeterogeneityMax = 0d;
	public int cpuHeterogeneityPopulation = 0;

	// IO Heterogeneity
	public static Distribution[] ioHeterogeneityDistributionOfDC = {Distribution.NORMAL,Distribution.NORMAL};
	public static double[] ioHeterogeneityCVOfDC = {0.15,0.15};
	public static int[] ioHeterogeneityAlphaOfDC = {0,0};
	public static double[] ioHeterogeneityBetaOfDC = {0d,0d};
	public static double[] ioHeterogeneityShapeOfDC = {0d,0d};
	public static double[] ioHeterogeneityLocationOfDC = {0d,0d};
	public static double[] ioHeterogeneityShiftOfDC = {0d,0d};
	public static double[] ioHeterogeneityMinOfDC = {0d,0d};
	public static double[] ioHeterogeneityMaxOfDC = {0d,0d};
	public static int[] ioHeterogeneityPopulationOfDC = {0,0};
	
	
	// IO Default Heterogeneity
	public Distribution ioHeterogeneityDistribution = Distribution.NORMAL;
	public double ioHeterogeneityCV = 0.15;
	public int ioHeterogeneityAlpha = 0;
	public double ioHeterogeneityBeta = 0d;
	public double ioHeterogeneityShape = 0d;
	public double ioHeterogeneityLocation = 0d;
	public double ioHeterogeneityShift = 0d;
	public double ioHeterogeneityMin = 0d;
	public double ioHeterogeneityMax = 0d;
	public int ioHeterogeneityPopulation = 0;
	

	// BW Heterogeneity
	public static Distribution[] bwHeterogeneityDistributionOfDC = {Distribution.NORMAL,Distribution.NORMAL};
	public static double[] bwHeterogeneityCVOfDC = {0.2,0.2};
	public static int[] bwHeterogeneityAlphaOfDC = {0,0};
	public static double[] bwHeterogeneityBetaOfDC = {0d,0d};
	public static double[] bwHeterogeneityShapeOfDC = {0d,0d};
	public static double[] bwHeterogeneityLocationOfDC = {0d,0d};
	public static double[] bwHeterogeneityShiftOfDC = {0d,0d};
	public static double[] bwHeterogeneityMinOfDC = {0d,0d};
	public static double[] bwHeterogeneityMaxOfDC = {0d,0d};
	public static int[] bwHeterogeneityPopulationOfDC = {0,0};
	
	
	// BW Default Heterogeneity
	public Distribution bwHeterogeneityDistribution = Distribution.NORMAL;
	public double bwHeterogeneityCV = 0.2;
	public int bwHeterogeneityAlpha = 0;
	public double bwHeterogeneityBeta = 0d;
	public double bwHeterogeneityShape = 0d;
	public double bwHeterogeneityLocation = 0d;
	public double bwHeterogeneityShift = 0d;
	public double bwHeterogeneityMin = 0d;
	public double bwHeterogeneityMax = 0d;
	public int bwHeterogeneityPopulation = 0;
	
	
	public void setDCHeterogeneity(
			Distribution cpuHeterogeneityDistribution,
			double cpuHeterogeneityCV,
			int cpuHeterogeneityAlpha,
			double cpuHeterogeneityBeta,
			double cpuHeterogeneityShape,
			double cpuHeterogeneityLocation,
			double cpuHeterogeneityShift,
			double cpuHeterogeneityMin,
			double cpuHeterogeneityMax,
			int cpuHeterogeneityPopulation,
			Distribution ioHeterogeneityDistribution,
			double ioHeterogeneityCV,
			int ioHeterogeneityAlpha,
			double ioHeterogeneityBeta,
			double ioHeterogeneityShape,
			double ioHeterogeneityLocation,
			double ioHeterogeneityShift,
			double ioHeterogeneityMin,
			double ioHeterogeneityMax,
			int ioHeterogeneityPopulation,
			Distribution bwHeterogeneityDistribution,
			double bwHeterogeneityCV,
			int bwHeterogeneityAlpha,
			double bwHeterogeneityBeta,
			double bwHeterogeneityShape,
			double bwHeterogeneityLocation,
			double bwHeterogeneityShift,
			double bwHeterogeneityMin,
			double bwHeterogeneityMax,
			int bwHeterogeneityPopulation,
			int[] nOpteronOfMachineType
			) {
		this.cpuHeterogeneityDistribution = cpuHeterogeneityDistribution;
		this.cpuHeterogeneityCV = cpuHeterogeneityCV;
		this.cpuHeterogeneityAlpha = cpuHeterogeneityAlpha;
		this.cpuHeterogeneityBeta = cpuHeterogeneityBeta;
		this.cpuHeterogeneityShape = cpuHeterogeneityShape;
		this.cpuHeterogeneityLocation = cpuHeterogeneityLocation;
		this.cpuHeterogeneityShift = cpuHeterogeneityShift;
		this.cpuHeterogeneityMin = cpuHeterogeneityMin;
		this.cpuHeterogeneityMax = cpuHeterogeneityMax;
		this.cpuHeterogeneityPopulation = cpuHeterogeneityPopulation;
		
		this.ioHeterogeneityDistribution = ioHeterogeneityDistribution;
		this.ioHeterogeneityCV = ioHeterogeneityCV;
		this.ioHeterogeneityAlpha = ioHeterogeneityAlpha;
		this.ioHeterogeneityBeta = ioHeterogeneityBeta;
		this.ioHeterogeneityShape = ioHeterogeneityShape;
		this.ioHeterogeneityLocation = ioHeterogeneityLocation;
		this.ioHeterogeneityShift = ioHeterogeneityShift;
		this.ioHeterogeneityMin = ioHeterogeneityMin;
		this.ioHeterogeneityMax = ioHeterogeneityMax;
		this.ioHeterogeneityPopulation = ioHeterogeneityPopulation;
		
		this.bwHeterogeneityDistribution = bwHeterogeneityDistribution;
		this.bwHeterogeneityCV = bwHeterogeneityCV;
		this.bwHeterogeneityAlpha = bwHeterogeneityAlpha;
		this.bwHeterogeneityBeta = bwHeterogeneityBeta;
		this.bwHeterogeneityShape = bwHeterogeneityShape;
		this.bwHeterogeneityLocation = bwHeterogeneityLocation;
		this.bwHeterogeneityShift = bwHeterogeneityShift;
		this.bwHeterogeneityMin = bwHeterogeneityMin;
		this.bwHeterogeneityMax = bwHeterogeneityMax;
		this.bwHeterogeneityPopulation = bwHeterogeneityPopulation;
		
		this.nOpteronOfMachineType = nOpteronOfMachineType;
	}

	// CPU Dynamics
	public static double[] cpuBaselineChangesPerHourOfDC = {0.5,0.5};
	public static Distribution[] cpuDynamicsDistributionOfDC = {Distribution.NORMAL,Distribution.NORMAL};
	public static double[] cpuDynamicsCVOfDC = {0.054,0.054};
	public static int[] cpuDynamicsAlphaOfDC = {0,0};
	public static double[] cpuDynamicsBetaOfDC = {0d,0d};
	public static double[] cpuDynamicsShapeOfDC = {0d,0d};
	public static double[] cpuDynamicsLocationOfDC = {0d,0d};
	public static double[] cpuDynamicsShiftOfDC = {0d,0d};
	public static double[] cpuDynamicsMinOfDC = {0d,0d};
	public static double[] cpuDynamicsMaxOfDC = {0d,0d};
	public static int[] cpuDynamicsPopulationOfDC = {0,0};

	// IO Dynamics
	public static double[] ioBaselineChangesPerHourOfDC = {0.5,0.5};
	public static Distribution[] ioDynamicsDistributionOfDC = {Distribution.NORMAL,Distribution.NORMAL};
	public static double[] ioDynamicsCVOfDC = {0.033,0.033};
	public static int[] ioDynamicsAlphaOfDC = {0,0};
	public static double[] ioDynamicsBetaOfDC = {0d,0d};
	public static double[] ioDynamicsShapeOfDC = {0d,0d};
	public static double[] ioDynamicsLocationOfDC = {0d,0d};
	public static double[] ioDynamicsShiftOfDC = {0d,0d};
	public static double[] ioDynamicsMinOfDC = {0d,0d};
	public static double[] ioDynamicsMaxOfDC = {0d,0d};
	public static int[] ioDynamicsPopulationOfDC = {0,0};

	// BW Dynamics
	public static double[] bwBaselineChangesPerHourOfDC = {0.5,0.5};
	public static Distribution[] bwDynamicsDistributionOfDC = {Distribution.NORMAL,Distribution.NORMAL};
	public static double[] bwDynamicsCVOfDC = {0.04,0.04};
	public static int[] bwDynamicsAlphaOfDC = {0,0};
	public static double[] bwDynamicsBetaOfDC = {0d,0d};
	public static double[] bwDynamicsShapeOfDC = {0d,0d};
	public static double[] bwDynamicsLocationOfDC = {0d,0d};
	public static double[] bwDynamicsShiftOfDC = {0d,0d};
	public static double[] bwDynamicsMinOfDC = {0d,0d};
	public static double[] bwDynamicsMaxOfDC = {0d,0d};
	public static int[] bwDynamicsPopulationOfDC = {0,0};

	// CPU noise
	public static Distribution[] cpuNoiseDistributionOfDC = {Distribution.NORMAL,Distribution.NORMAL};
	public static double[] cpuNoiseCVOfDC = {0.028,0.028};
	public static int[] cpuNoiseAlphaOfDC = {0,0};
	public static double[] cpuNoiseBetaOfDC = {0d,0d};
	public static double[] cpuNoiseShapeOfDC = {0d,0d};
	public static double[] cpuNoiseLocationOfDC = {0d,0d};
	public static double[] cpuNoiseShiftOfDC = {0d,0d};
	public static double[] cpuNoiseMinOfDC = {0d,0d};
	public static double[] cpuNoiseMaxOfDC = {0d,0d};
	public static int[] cpuNoisePopulationOfDC = {0,0};

	// IO noise
	public static Distribution[] ioNoiseDistributionOfDC = {Distribution.NORMAL,Distribution.NORMAL};
	public static double[] ioNoiseCVOfDC = {0.007,0.007};
	public static int[] ioNoiseAlphaOfDC = {0,0};
	public static double[] ioNoiseBetaOfDC = {0d,0d};
	public static double[] ioNoiseShapeOfDC = {0d,0d};
	public static double[] ioNoiseLocationOfDC = {0d,0d};
	public static double[] ioNoiseShiftOfDC = {0d,0d};
	public static double[] ioNoiseMinOfDC = {0d,0d};
	public static double[] ioNoiseMaxOfDC = {0d,0d};
	public static int[] ioNoisePopulationOfDC = {0,0};

	// BW noise
	public static Distribution[] bwNoiseDistributionOfDC = {Distribution.NORMAL,Distribution.NORMAL};
	public static double[] bwNoiseCVOfDC = {0.01,0.01};
	public static int[] bwNoiseAlphaOfDC = {0,0};
	public static double[] bwNoiseBetaOfDC = {0d,0d};
	public static double[] bwNoiseShapeOfDC = {0d,0d};
	public static double[] bwNoiseLocationOfDC = {0d,0d};
	public static double[] bwNoiseShiftOfDC = {0d,0d};
	public static double[] bwNoiseMinOfDC = {0d,0d};
	public static double[] bwNoiseMaxOfDC = {0d,0d};
	public static int[] bwNoisePopulationOfDC = {0,0};

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
	
	
	//upperbound of datasize
	public static long ubOfDataSize = 2048L * 1024L * 1024L; // 2G
	
	//lowerbound of datasize
	public static long lbOfDataSize = 128L * 1024L; // 128K
	
	
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
			result[dcindex] = 0;
			for (int typeindex = 0; typeindex < machineType; typeindex++) {
				result[dcindex] += machineDis[typeindex]*(double)iopsPerPeOfMachineType[typeindex];
			}
		}
		return result;
	}

	private static double[] getBwBaseline() {
		double[] result = new double[numberOfDC];
		
		for (int dcindex = 0; dcindex < numberOfDC; dcindex++) {
			double[] machineDis = getMachineDis(dcindex);
			result[dcindex] = 0;
			for (int typeindex = 0; typeindex < machineType; typeindex++) {
				result[dcindex] += machineDis[typeindex]*(double)bwpsPerPeOfMachineType[typeindex];
			}
		}
		return result;
	}

	public static double[] getMachineDis(int dcindex) {
		double[] result = new double[machineType] ;
		double sum = 0;
		for (int typeindex = 0; typeindex < machineType; typeindex++) {
			sum += (double)nOpteronOfMachineTypeOfDC[dcindex][typeindex];
		}
		for (int typeindex = 0; typeindex < machineType; typeindex++) {
			result[typeindex] = (double)nOpteronOfMachineTypeOfDC[dcindex][typeindex]/sum;
		}
		return result;
	}
	
	
	
	
	public static double[] getMIPSBaseline() {
		double[] result = new double[numberOfDC];
		
		for (int dcindex = 0; dcindex < numberOfDC; dcindex++) {
			double[] machineDis = getMachineDis(dcindex);
			result[dcindex] = 0;
			for (int typeindex = 0; typeindex < machineType; typeindex++) {
				result[dcindex] += machineDis[typeindex]*(double)mipsPerCoreOpteronOfMachineType[typeindex];
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
