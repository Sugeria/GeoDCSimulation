package de.huberlin.wbi.dcs.examples;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.distributions.ExponentialDistr;
import org.cloudbus.cloudsim.distributions.GammaDistr;
import org.cloudbus.cloudsim.distributions.LognormalDistr;
import org.cloudbus.cloudsim.distributions.LomaxDistribution;
import org.cloudbus.cloudsim.distributions.ParetoDistr;
import org.cloudbus.cloudsim.distributions.UniformDistr;
import org.cloudbus.cloudsim.distributions.WeibullDistr;
import org.cloudbus.cloudsim.distributions.ZipfDistr;
import org.cloudbus.cloudsim.network.DelayMatrix_Float;
import org.cloudbus.cloudsim.network.GraphReaderBrite;
import org.cloudbus.cloudsim.network.TopologicalGraph;
import org.workflowsim.Job;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;

import com.mathworks.toolbox.javabuilder.*;

import de.huberlin.wbi.dcs.distributions.NormalDistribution;
import de.huberlin.wbi.dcs.workflow.Task;
import taskAssign.TaskAssign;
import wtt.info.extractor.ModelExtractor;
import wtt.info.generator.ModelGenerator;

public class Parameters {
	
	
    /*
     * Scheduling Algorithm (Local Scheduling Algorithm)
     */

    public enum SchedulingAlgorithm {

        MAXMIN, MINMIN, MCT, DATA, 
        STATIC, FCFS, ROUNDROBIN, INVALID,MIN, MINRATE
    }
    
    /**
     * Planning Algorithm (Global Scheduling Algorithm)
     * 
     */
    public enum PlanningAlgorithm{
        INVALID, RANDOM, HEFT, DHEFT
    }
    
    /**
     * File Type
     */
    public enum FileType{
        NONE(0), INPUT(1), OUTPUT(2);
        public final int value;
        private FileType(int fType){
            this.value = fType;
        }
    }
    
    /**
     * File Type
     */
    public enum ClassType{
        STAGE_IN(1), COMPUTE(2), STAGE_OUT(3), CLEAN_UP(4);
        public final int value;
        private ClassType(int cType){
            this.value = cType;
        }
    }
    
    /**
     * The cost model
     * DATACENTER: specify the cost per data center
     * VM: specify the cost per VM
     */
    public enum CostModel{
        DATACENTER(1), VM(2);
        public final int value;
        private CostModel(int model){
            this.value = model;
        }
    }
    
    /** 
     * Source Host (submit host)
     */
    public static String SOURCE = "source";
    
    public static final int BASE = 0;
    
    /**
     * Scheduling mode
     */
    private static SchedulingAlgorithm schedulingAlgorithm;
    
    /**
     * Planning mode
     */
    private static PlanningAlgorithm planningAlgorithm;
    
    /**
     * Reducer mode
     */
    private static String reduceMethod;
    /**
     * Number of vms available
     */
    private static int vmNum;
    /**
     * The physical path to DAX file
     */
    private static String daxPath;
    
    /**
     * The physical path to DAX files
     */
    private static List<String> daxPaths;
    /**
     * The physical path to runtime file In the runtime file, please use format
     * as below ID1 1.0 ID2 2.0 ... This is optional, if you have specified task
     * runtime in DAX then you don't need to specify this file
     */
    private static String runtimePath;
    /**
     * The physical path to datasize file In the datasize file, please use
     * format as below DATA1 1000 DATA2 2000 ... This is optional, if you have
     * specified datasize in DAX then you don't need to specify this file
     */
    private static String datasizePath;
    /**
     * Version number
     */
    private static final String version = "1.1.0";
    /**
     * Note information
     */
    private static final String note = " supports planning algorithm at Nov 9, 2013";
    /**
     * Overhead parameters
     */
    private static OverheadParameters oParams;
    /**
     * Clustering parameters
     */
    private static ClusteringParameters cParams;
    /**
     * Deadline of a workflow
     */
    private static long deadline;
    
    /**
     * the bandwidth from one vm to one vm
     */
    private static double[][] bandwidths;
    
    
    /**
     * The maximum depth. It is inited manually and used in FailureGenerator
     */
    private static int maxDepth;
    
    /**
     * Invalid String
     */
    private static final String INVALID = "Invalid";
    
    /**
     * The scale of runtime. Multiple runtime by this
     */
    private static double runtime_scale = 1.0;
    
    /**
     * The default cost model is based on datacenter, similar to CloudSim
     */
    private static CostModel costModel = CostModel.DATACENTER;
    
    /**
     * A static function so that you can specify them in any place
     *
     * @param vm, the number of vms
     * @param dax, the DAX path
     * @param runtime, optional, the runtime file path
     * @param datasize, optional, the datasize file path
     * @param op, overhead parameters
     * @param cp, clustering parameters
     * @param scheduler, scheduling mode
     * @param planner, planning mode
     * @param rMethod , reducer mode
     * @param dl, deadline
     */
    public static void init(
            int vm, String dax, String runtime, String datasize,
            OverheadParameters op, ClusteringParameters cp,
            SchedulingAlgorithm scheduler, PlanningAlgorithm planner, String rMethod,
            long dl) {

        cParams = cp;
        vmNum = vm;
        daxPath = dax;
        runtimePath = runtime;
        datasizePath = datasize;

        oParams = op;
        schedulingAlgorithm = scheduler;
        planningAlgorithm = planner;
        reduceMethod = rMethod;
        deadline = dl;
        maxDepth = 0;
//        generateWorkflow();
//        setInfoAmongDC();
//        createInfoOfDC();
//        try {
//			ModelGenerator.save();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
        try {
			ModelExtractor.extracte();
		} catch (IOException e) {
			e.printStackTrace();
		}
        MIPSbaselineOfDC = getMIPSBaseline();
    	bwBaselineOfDC = getBwBaseline();
    	ioBaselineOfDC = getIoBaseline();
    	nVms = sumOfVM();
    }
    
    /**
     * A static function so that you can specify them in any place
     *
     * @param vm, the number of vms
     * @param dax, the list of DAX paths 
     * @param runtime, optional, the runtime file path
     * @param datasize, optional, the datasize file path
     * @param op, overhead parameters
     * @param cp, clustering parameters
     * @param scheduler, scheduling mode
     * @param planner, planning mode
     * @param rMethod , reducer mode
     * @param dl, deadline of a workflow
     */
//    public void init(
//            int vm, List<String> dax, String runtime, String datasize,
//            OverheadParameters op, ClusteringParameters cp,
//            SchedulingAlgorithm scheduler, PlanningAlgorithm planner, String rMethod,
//            long dl) {
//
//        cParams = cp;
//        vmNum = vm;
//        //daxPaths = dax;
//        runtimePath = runtime;
//        datasizePath = datasize;
//
//        oParams = op;
//        schedulingAlgorithm = scheduler;
//        planningAlgorithm = planner;
//        reduceMethod = rMethod;
//        deadline = dl;
//        maxDepth = 0;
//        generateWorkflow();
//        setInfoAmongDC();
//        createInfoOfDC();
//        MIPSbaselineOfDC = getMIPSBaseline();
//    	bwBaselineOfDC = getBwBaseline();
//    	ioBaselineOfDC = getIoBaseline();
//    	nVms = sumOfVM();
//    }

    /**
     * Gets the overhead parameters
     *
     * @return the overhead parameters
     * @pre $none
     * @post $none
     */
    public static OverheadParameters getOverheadParams() {
        return oParams;
    }

    

    /**
     * Gets the reducer mode
     *
     * @return the reducer
     * @pre $none
     * @post $none
     */
    public static String getReduceMethod() {
        if(reduceMethod!=null){
            return reduceMethod;
        }else{
            return INVALID;
        }
    }

   

    /**
     * Gets the DAX path
     *
     * @return the DAX path
     * @pre $none
     * @post $none
     */
    public static String getDaxPath() {
        return daxPath;
    }

    /**
     * Gets the runtime file path
     *
     * @return the runtime file path
     * @pre $none
     * @post $none
     */
    public static String getRuntimePath() {
        return runtimePath;
    }

    /**
     * Gets the data size path
     *
     * @return the datasize file path
     * @pre $none
     * @post $none
     */
    public static String getDatasizePath() {
        return datasizePath;
    }

    
    /**
     * Gets the vm number
     *
     * @return the vm number
     * @pre $none
     * @post $none
     */
    public static int getVmNum() {
        return vmNum;
    }

    
    /**
     * Gets the cost model
     * 
     * @return costModel
     */
    public static CostModel getCostModel(){
        return costModel;
    }
    
    /**
     * Sets the vm number
     *
     * @param num
     */
    public static void setVmNum(int num) {
        vmNum = num;
    }

    /**
     * Gets the clustering parameters
     *
     * @return the clustering parameters
     */
    public static ClusteringParameters getClusteringParameters() {
        return cParams;
    }

    /**
     * Gets the scheduling method
     *
     * @return the scheduling method
     */
    public static SchedulingAlgorithm getSchedulingAlgorithm() {
        return schedulingAlgorithm;
    }
    
    /**
     * Gets the planning method
     * @return the planning method
     * 
     */
    public static PlanningAlgorithm getPlanningAlgorithm() {
        return planningAlgorithm;
    }
    /**
     * Gets the version
     * @return version
     */
    public static String getVersion(){
        return version;
    }

    public static void printVersion() {
        Log.printLine("WorkflowSim Version: " + version);
        Log.printLine("Change Note: " + note);
    }
    /*
     * Gets the deadline
     */
    public static long getDeadline(){
    	return deadline;
    }
    
    /**
     * Gets the maximum depth
     * @return the maxDepth
     */
    public static int getMaxDepth(){
        return maxDepth;
    }
    
    /**
     * Sets the maximum depth
     * @param depth the maxDepth
     */
    public static void setMaxDepth(int depth){
        maxDepth = depth;
    }
    
    /**
     * Sets the runtime scale
     * @param scale 
     */
    public static void setRuntimeScale(double scale){
        runtime_scale = scale;
    }
    
    /**
     * Sets the cost model
     * @param model
     */
    public static void setCostModel(CostModel model){
        costModel = model;
    }
    
    /**
     * Gets the runtime scale
     * @return 
     */
    public static double getRuntimeScale(){
        return runtime_scale;
    }
    
    /**
     * Gets the dax paths
     * @return 
     */
    public static List<String> getDAXPaths() {
        return daxPaths;
    }
	
    
    public static String[] workflowCandidate = {
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/CyberShake_50.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/CyberShake_100.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Epigenomics_24.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Epigenomics_46.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Epigenomics_100.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Inspiral_30.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Inspiral_50.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Inspiral_100.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Montage_25.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Montage_50.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Montage_100.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Sipht_30.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Sipht_60.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Sipht_100.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/CyberShake_1000.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Epigenomics_997.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Inspiral_1000.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Montage_1000.xml",
    		"C:/Users/han/git/GeoDCSimulation/dynamiccloudsim/config/dax/Sipht_1000.xml"
    };
    public static String[] workflow_relative_Candidate = {
    		"./dynamiccloudsim/config/dax/CyberShake_50.xml",
    		"./dynamiccloudsim/config/dax/CyberShake_100.xml",
    		"./dynamiccloudsim/config/dax/Epigenomics_24.xml",
    		"./dynamiccloudsim/config/dax/Epigenomics_46.xml",
    		"./dynamiccloudsim/config/dax/Epigenomics_100.xml",
    		"./dynamiccloudsim/config/dax/Inspiral_30.xml",
    		"./dynamiccloudsim/config/dax/Inspiral_50.xml",
    		"./dynamiccloudsim/config/dax/Inspiral_100.xml",
    		"./dynamiccloudsim/config/dax/Montage_25.xml",
    		"./dynamiccloudsim/config/dax/Montage_50.xml",
    		"./dynamiccloudsim/config/dax/Montage_100.xml",
    		"./dynamiccloudsim/config/dax/Sipht_30.xml",
    		"./dynamiccloudsim/config/dax/Sipht_60.xml",
    		"./dynamiccloudsim/config/dax/Sipht_100.xml",
    		"./dynamiccloudsim/config/dax/CyberShake_1000.xml",
    		"./dynamiccloudsim/config/dax/Epigenomics_997.xml",
    		"./dynamiccloudsim/config/dax/Inspiral_1000.xml",
    		"./dynamiccloudsim/config/dax/Montage_1000.xml",
    		"./dynamiccloudsim/config/dax/Sipht_1000.xml"
    };
    
    
	
	// workflow
    // default 5 workflows each minutes
    public static double lambda = 0.083;
    // default 3 days workflow
    // defend time exceed INT.MAX_VALUE
    public static double seconds = 10;
    
    public static Map<Double, List<String>> workflowArrival;
    
    private static void generateWorkflow() {
    	TaskAssign taskassign = null;
		MWNumericArray lambda_para = null;
		MWNumericArray time_para = null;
		Object[] result = null;
		workflowArrival = new HashMap<>();
		int[] x = null;
		try {
			taskassign = new TaskAssign();
			int[] dims = {1,1};
			int[] pos = {1,1};
			lambda_para = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
			lambda_para.set(pos, lambda);
			time_para = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
			time_para.set(pos, seconds);
			result = taskassign.Poisson_series(1,lambda_para,time_para);
			x = ((MWNumericArray)result[0]).getIntData();
			int secondtime = x.length;
			for(int timeindex = 1; timeindex < secondtime ; timeindex++) {
				if(x[timeindex] == 0) {
					continue;
				}else {
					List<String> workflowFileName = new ArrayList<>();
					for (int workflowindex = 0; workflowindex < x[timeindex]; workflowindex++) {
						// generate workflows from candidate
						double workflowSizeProb = Math.random();
						if(workflowSizeProb < 0.89) {
							// 0-13
							int candidateIndex = (int)(Math.random() * 13);
							workflowFileName.add(workflow_relative_Candidate[candidateIndex]);
						}else if(workflowSizeProb < 0.11) {
							// 14-18
							int candidateIndex = (int)(Math.random() * 4 + 14);
							workflowFileName.add(workflow_relative_Candidate[candidateIndex]);
						}
					} 
					workflowArrival.put((double)timeindex, workflowFileName);
					
				}
			}
		} catch (MWException e) {
			e.printStackTrace();
		}finally {
			MWNumericArray.disposeArray(taskassign);
			MWNumericArray.disposeArray(lambda_para);
			MWNumericArray.disposeArray(time_para);
			MWNumericArray.disposeArray(result);
			if(taskassign != null) {
				taskassign.dispose();
			}
		}
		
	}
	

    public static double r = 0.2d;
    
    public static double epsilon = 0.4d;
    
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
//			int cpuHeterogeneityAlpha,
//			double cpuHeterogeneityBeta,
//			double cpuHeterogeneityShape,
//			double cpuHeterogeneityLocation,
//			double cpuHeterogeneityShift,
//			double cpuHeterogeneityMin,
//			double cpuHeterogeneityMax,
//			int cpuHeterogeneityPopulation,
			Distribution ioHeterogeneityDistribution,
			double ioHeterogeneityCV,
//			int ioHeterogeneityAlpha,
//			double ioHeterogeneityBeta,
//			double ioHeterogeneityShape,
//			double ioHeterogeneityLocation,
//			double ioHeterogeneityShift,
//			double ioHeterogeneityMin,
//			double ioHeterogeneityMax,
//			int ioHeterogeneityPopulation,
			Distribution bwHeterogeneityDistribution,
			double bwHeterogeneityCV,
//			int bwHeterogeneityAlpha,
//			double bwHeterogeneityBeta,
//			double bwHeterogeneityShape,
//			double bwHeterogeneityLocation,
//			double bwHeterogeneityShift,
//			double bwHeterogeneityMin,
//			double bwHeterogeneityMax,
//			int bwHeterogeneityPopulation,
			int[] nOpteronOfMachineType
			) {
		this.cpuHeterogeneityDistribution = cpuHeterogeneityDistribution;
		this.cpuHeterogeneityCV = cpuHeterogeneityCV;
//		this.cpuHeterogeneityAlpha = cpuHeterogeneityAlpha;
//		this.cpuHeterogeneityBeta = cpuHeterogeneityBeta;
//		this.cpuHeterogeneityShape = cpuHeterogeneityShape;
//		this.cpuHeterogeneityLocation = cpuHeterogeneityLocation;
//		this.cpuHeterogeneityShift = cpuHeterogeneityShift;
//		this.cpuHeterogeneityMin = cpuHeterogeneityMin;
//		this.cpuHeterogeneityMax = cpuHeterogeneityMax;
//		this.cpuHeterogeneityPopulation = cpuHeterogeneityPopulation;
		
		this.ioHeterogeneityDistribution = ioHeterogeneityDistribution;
		this.ioHeterogeneityCV = ioHeterogeneityCV;
//		this.ioHeterogeneityAlpha = ioHeterogeneityAlpha;
//		this.ioHeterogeneityBeta = ioHeterogeneityBeta;
//		this.ioHeterogeneityShape = ioHeterogeneityShape;
//		this.ioHeterogeneityLocation = ioHeterogeneityLocation;
//		this.ioHeterogeneityShift = ioHeterogeneityShift;
//		this.ioHeterogeneityMin = ioHeterogeneityMin;
//		this.ioHeterogeneityMax = ioHeterogeneityMax;
//		this.ioHeterogeneityPopulation = ioHeterogeneityPopulation;
		
		this.bwHeterogeneityDistribution = bwHeterogeneityDistribution;
		this.bwHeterogeneityCV = bwHeterogeneityCV;
//		this.bwHeterogeneityAlpha = bwHeterogeneityAlpha;
//		this.bwHeterogeneityBeta = bwHeterogeneityBeta;
//		this.bwHeterogeneityShape = bwHeterogeneityShape;
//		this.bwHeterogeneityLocation = bwHeterogeneityLocation;
//		this.bwHeterogeneityShift = bwHeterogeneityShift;
//		this.bwHeterogeneityMin = bwHeterogeneityMin;
//		this.bwHeterogeneityMax = bwHeterogeneityMax;
//		this.bwHeterogeneityPopulation = bwHeterogeneityPopulation;

		this.nOpteronOfMachineType = nOpteronOfMachineType;
	}
	
	


	// Delay among DC
	public static float[][] delayAmongDCIndex;
	public static int[] degreeNumberOfDC;
	
	// use the brite file to generate the delay among DCs
	private static void setInfoAmongDC(){
		delayAmongDCIndex = new float[Parameters.numberOfDC][Parameters.numberOfDC];
		GraphReaderBrite brite = new GraphReaderBrite();
		TopologicalGraph topograph = null;
		try {
			topograph = brite.readGraphFile("./dynamiccloudsim/world.brite");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		degreeNumberOfDC = new int[numberOfDC];
		DelayMatrix_Float delayMatrix = new DelayMatrix_Float(topograph, true);
		for(int dci = 0; dci < Parameters.numberOfDC; dci++) {
			for(int dcj = 0; dcj < Parameters.numberOfDC; dcj++) {
				delayAmongDCIndex[dci][dcj] = delayMatrix.getDelay(dci,dcj);
			}
			degreeNumberOfDC[dci] = delayMatrix.getDegree(dci);
		}
		
		
	}
	
	// create DC parameters
	
	// 0.05 Large
	// 0.2 Medium
	// 0.75 Small
	
	private static void createInfoOfDC() {
		
		nOpteronOfMachineTypeOfDC = new int[numberOfDC][machineType];
		cpuHeterogeneityDistributionOfDC = new Distribution[numberOfDC];
		cpuHeterogeneityCVOfDC = new double[numberOfDC];
		ioHeterogeneityDistributionOfDC = new Distribution[numberOfDC];
		ioHeterogeneityCVOfDC = new double[numberOfDC];
		bwHeterogeneityDistributionOfDC = new Distribution[numberOfDC];
		bwHeterogeneityCVOfDC = new double[numberOfDC];
		cpuDynamicsDistributionOfDC = new Distribution[numberOfDC];
		cpuDynamicsCVOfDC = new double[numberOfDC];
		cpuBaselineChangesPerHourOfDC = new double[numberOfDC];
		ioDynamicsDistributionOfDC = new Distribution[numberOfDC];
		ioDynamicsCVOfDC = new double[numberOfDC];
		ioBaselineChangesPerHourOfDC = new double[numberOfDC];
		bwDynamicsDistributionOfDC = new Distribution[numberOfDC];
		bwDynamicsCVOfDC = new double[numberOfDC];
		bwBaselineChangesPerHourOfDC = new double[numberOfDC];
		cpuNoiseDistributionOfDC = new Distribution[numberOfDC];
		cpuNoiseCVOfDC = new double[numberOfDC];
		ioNoiseDistributionOfDC = new Distribution[numberOfDC];
		ioNoiseCVOfDC = new double[numberOfDC];
		bwNoiseDistributionOfDC = new Distribution[numberOfDC];
		bwNoiseCVOfDC = new double[numberOfDC];
		likelihoodOfStragglerOfDC = new double[numberOfDC];
		stragglerPerformanceCoefficientOfDC = new double[numberOfDC];
		likelihoodOfFailure = new double[numberOfDC];
		runtimeFactorInCaseOfFailure = new double[numberOfDC];
		likelihoodOfDCFailure = new double[numberOfDC];
		uplinkOfDC = new double[numberOfDC];
		downlinkOfDC = new double[numberOfDC];
		numberOfVMperDC = new int[numberOfDC];
		// degree rank
		MWNumericArray degreelist_para = null;
		MWNumericArray B_out = null;
		MWNumericArray I_out = null;
		TaskAssign taskAssign = null;
		Object[] result = null;
		int[] B = null;
		int[] I = null;
		
		try {
			taskAssign = new TaskAssign();
			int[] dim = {1,numberOfDC};
			int[] pos =  new int[2];
			pos[0] = 1;
			degreelist_para = MWNumericArray.newInstance(dim, MWClassID.INT32, MWComplexity.REAL);
			for(int dcindex = 0; dcindex < numberOfDC; dcindex++) {
				pos[1] = dcindex + 1;
				degreelist_para.set(pos, degreeNumberOfDC[dcindex]);
			}
			
			result = taskAssign.degree_rank(2,degreelist_para);
			B_out = (MWNumericArray)result[0];
			B = B_out.getIntData();
			I_out = (MWNumericArray)result[1];
			I = I_out.getIntData();
			
		} catch (MWException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			MWNumericArray.disposeArray(degreelist_para);
			MWNumericArray.disposeArray(B_out);
			MWNumericArray.disposeArray(I_out);
			MWNumericArray.disposeArray(result);
			if(taskAssign != null) {
				taskAssign.dispose();
			}
		}
		
		
		int Large_part = (int)(numberOfDC * 0.05);
		int Medium_part = (int)(numberOfDC * 0.2) + Large_part;
		int dcindex = 0;
		for(int dccounter = 0; dccounter < numberOfDC; dccounter++) {
			dcindex = I[dccounter]-1;
			if(dccounter < Large_part) {
				//Large DC
				int Type = (int)(Math.random()*machineType);
				for(int typeindex = 0; typeindex < machineType; typeindex++) {
					if(Type == typeindex) {
						nOpteronOfMachineTypeOfDC[dcindex][typeindex] = (int)(Math.random()*1500 + 1500);
						numberOfVMperDC[dcindex] = nOpteronOfMachineTypeOfDC[dcindex][typeindex];
					}else {
						nOpteronOfMachineTypeOfDC[dcindex][typeindex] = 0;
					}
				}
				cpuHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.3 0.4 0.6
				cpuHeterogeneityCVOfDC[dcindex] = Math.random()*(0.3);
				ioHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.15 0.20 0.3
				ioHeterogeneityCVOfDC[dcindex] = Math.random()*0.15;
				bwHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.2 0.25 0.4
				bwHeterogeneityCVOfDC[dcindex] = Math.random()*0.2;
				cpuDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.054 0.06 0.09
				cpuDynamicsCVOfDC[dcindex] = Math.random()*0.054;
				// 0.5 0.6 0.9
				cpuBaselineChangesPerHourOfDC[dcindex] = Math.random()*0.5;
				ioDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.033 0.053 0.073
				ioDynamicsCVOfDC[dcindex] = Math.random()*0.033;
				// 0.5 0.6 0.7
				ioBaselineChangesPerHourOfDC[dcindex] = Math.random()*0.5;
				bwDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.04 0.06 0.09
				bwDynamicsCVOfDC[dcindex] = Math.random()*0.04;
				// 0.5 0.7 0.8
				bwBaselineChangesPerHourOfDC[dcindex] = Math.random()*0.5;
				cpuNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.028 0.04 0.08
				cpuNoiseCVOfDC[dcindex] = Math.random()*0.028;
				ioNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.007 0.009 0.012
				ioNoiseCVOfDC[dcindex] = Math.random()*0.007;
				bwNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.01 0.03 0.06
				bwNoiseCVOfDC[dcindex] = Math.random()*0.01;
				
				// 0.015 0.020 0.025
				likelihoodOfStragglerOfDC[dcindex] = Math.random()*0.015;
				// 0.5 0.3 0.1;
				stragglerPerformanceCoefficientOfDC[dcindex] = Math.random()*0.5;
				
				// 0.002 0.008 0.011
				likelihoodOfFailure[dcindex] = Math.random()*0.002;
				// 10d 13d 20d
				runtimeFactorInCaseOfFailure[dcindex] = Math.random()*10d;
				
				// 0.0001 0.0002 0.0004
				likelihoodOfDCFailure[dcindex] = Math.random()*0.0001;
				
				// 0.55 0.60 0.85
				uplinkOfDC[dcindex] = Math.random()*nOpteronOfMachineTypeOfDC[dcindex][Type]*200*1024*0.55;
				downlinkOfDC[dcindex] = Math.random()*nOpteronOfMachineTypeOfDC[dcindex][Type]*200*1024*0.55;
				
				
			}else if(dccounter < Medium_part) {
				//Medium DC
				int Type = (int)(Math.random()*machineType);
				for(int typeindex = 0; typeindex < machineType; typeindex++) {
					if(Type == typeindex) {
						nOpteronOfMachineTypeOfDC[dcindex][typeindex] = (int)(Math.random()*1000 + 500);
						numberOfVMperDC[dcindex] = nOpteronOfMachineTypeOfDC[dcindex][typeindex];
					}else {
						nOpteronOfMachineTypeOfDC[dcindex][typeindex] = 0;
					}
				}
				cpuHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.3 0.4 0.6
				cpuHeterogeneityCVOfDC[dcindex] = Math.random()*(0.4-0.3)+0.3;
				ioHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.15 0.20 0.3
				ioHeterogeneityCVOfDC[dcindex] = Math.random()*(0.20-0.15)+0.15;
				bwHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.2 0.25 0.4
				bwHeterogeneityCVOfDC[dcindex] = Math.random()*(0.25-0.2)+0.2;
				cpuDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.054 0.06 0.09
				cpuDynamicsCVOfDC[dcindex] = Math.random()*(0.06-0.054)+0.054;
				// 0.5 0.6 0.9
				cpuBaselineChangesPerHourOfDC[dcindex] = Math.random()*(0.6-0.5)+0.5;
				ioDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.033 0.053 0.073
				ioDynamicsCVOfDC[dcindex] = Math.random()*(0.053-0.033)+0.033;
				// 0.5 0.6 0.7
				ioBaselineChangesPerHourOfDC[dcindex] = Math.random()*(0.6-0.5)+0.5;
				bwDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.04 0.06 0.09
				bwDynamicsCVOfDC[dcindex] = Math.random()*(0.06-0.04)+0.04;
				// 0.5 0.7 0.8
				bwBaselineChangesPerHourOfDC[dcindex] = Math.random()*(0.7-0.5)+0.5;
				cpuNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.028 0.04 0.08
				cpuNoiseCVOfDC[dcindex] = Math.random()*(0.04-0.028)+0.028;
				ioNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.007 0.009 0.012
				ioNoiseCVOfDC[dcindex] = Math.random()*(0.009-0.007)+0.007;
				bwNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.01 0.03 0.06
				bwNoiseCVOfDC[dcindex] = Math.random()*(0.03-0.01)+0.01;
				
				// 0.015 0.020 0.025
				likelihoodOfStragglerOfDC[dcindex] = Math.random()*(0.020-0.015)+0.015;
				// 0.5 0.3 0.1;
				stragglerPerformanceCoefficientOfDC[dcindex] = Math.random()*(0.5-0.3)+0.3;
				
				// 0.002 0.008 0.011
				likelihoodOfFailure[dcindex] = Math.random()*(0.008-0.002)+0.002;
				// 10d 13d 20d
				runtimeFactorInCaseOfFailure[dcindex] = Math.random()*(13d-10d)+10d;
				
				// 0.0001 0.0002 0.0004
				likelihoodOfDCFailure[dcindex] = Math.random()*(0.0002-0.0001)+0.0001;
				
				// 0.55 0.60 0.85
				uplinkOfDC[dcindex] = Math.random()*nOpteronOfMachineTypeOfDC[dcindex][Type]*200*1024*(0.60-0.55)+
						nOpteronOfMachineTypeOfDC[dcindex][Type]*200*1024*0.55;
				downlinkOfDC[dcindex] = Math.random()*nOpteronOfMachineTypeOfDC[dcindex][Type]*200*1024*(0.60-0.55)+
						nOpteronOfMachineTypeOfDC[dcindex][Type]*200*1024*0.55;
				
				
			}else {
				//Small DC
				int Type = (int)(Math.random()*machineType);
				for(int typeindex = 0; typeindex < machineType; typeindex++) {
					try {
						if(Type == typeindex) {
							nOpteronOfMachineTypeOfDC[dcindex][typeindex] = (int)(Math.random()*500 + 50);
							numberOfVMperDC[dcindex] = nOpteronOfMachineTypeOfDC[dcindex][typeindex];
						}else {
							
							nOpteronOfMachineTypeOfDC[dcindex][typeindex] = 0;
							
							
						}
					}catch (Exception e) {
						e.printStackTrace();
					}
					
				}
				cpuHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.3 0.4 0.6
				cpuHeterogeneityCVOfDC[dcindex] = Math.random()*(0.6-0.4)+0.4;
				ioHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.15 0.20 0.3
				ioHeterogeneityCVOfDC[dcindex] = Math.random()*(0.30-0.2)+0.2;
				bwHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.2 0.25 0.4
				bwHeterogeneityCVOfDC[dcindex] = Math.random()*(0.4-0.25)+0.25;
				cpuDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.054 0.06 0.09
				cpuDynamicsCVOfDC[dcindex] = Math.random()*(0.09-0.06)+0.06;
				// 0.5 0.6 0.9
				cpuBaselineChangesPerHourOfDC[dcindex] = Math.random()*(0.9-0.6)+0.6;
				ioDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.033 0.053 0.073
				ioDynamicsCVOfDC[dcindex] = Math.random()*(0.073-0.053)+0.053;
				// 0.5 0.6 0.7
				ioBaselineChangesPerHourOfDC[dcindex] = Math.random()*(0.7-0.6)+0.6;
				bwDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.04 0.06 0.09
				bwDynamicsCVOfDC[dcindex] = Math.random()*(0.09-0.06)+0.06;
				// 0.5 0.7 0.8
				bwBaselineChangesPerHourOfDC[dcindex] = Math.random()*(0.8-0.7)+0.7;
				cpuNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.028 0.04 0.08
				cpuNoiseCVOfDC[dcindex] = Math.random()*(0.08-0.04)+0.04;
				ioNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.007 0.009 0.012
				ioNoiseCVOfDC[dcindex] = Math.random()*(0.012-0.009)+0.009;
				bwNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
				// 0.01 0.03 0.06
				bwNoiseCVOfDC[dcindex] = Math.random()*(0.06-0.03)+0.03;
				
				// 0.015 0.020 0.025
				likelihoodOfStragglerOfDC[dcindex] = Math.random()*(0.025-0.020)+0.020;
				// 0.5 0.3 0.1;
				stragglerPerformanceCoefficientOfDC[dcindex] = Math.random()*(0.3-0.1)+0.1;
				
				// 0.002 0.008 0.011
				likelihoodOfFailure[dcindex] = Math.random()*(0.011-0.008)+0.008;
				// 10d 13d 20d
				runtimeFactorInCaseOfFailure[dcindex] = Math.random()*(20d-13d)+13d;
				
				// 0.0001 0.0002 0.0004
				likelihoodOfDCFailure[dcindex] = Math.random()*(0.0004-0.0002)+0.0002;
				
				// 0.55 0.60 0.85
				uplinkOfDC[dcindex] = Math.random()*nOpteronOfMachineTypeOfDC[dcindex][Type]*200*1024*(0.85-0.60)+
						nOpteronOfMachineTypeOfDC[dcindex][Type]*200*1024*0.60;
				downlinkOfDC[dcindex] = Math.random()*nOpteronOfMachineTypeOfDC[dcindex][Type]*200*1024*(0.85-0.60)+
						nOpteronOfMachineTypeOfDC[dcindex][Type]*200*1024*0.60;
				
			}
		}
		
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
	public static int numberOfDC = 10;
	
	
	// number of machineType in each datacenter
	public static int[][] nOpteronOfMachineTypeOfDC = new int[numberOfDC][machineType];
	
	
	
	
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
	public static double[] MIPSbaselineOfDC;
	public static double[] bwBaselineOfDC;
	public static double[] ioBaselineOfDC;
	
	
	//upperbound of datasize
	public static long ubOfDataSize = 128L * 1024L * 1024L; // 128M
	
	//lowerbound of datasize
	public static long lbOfDataSize = 128L * 1024L; // 128K
	
	
	// upperbound of inputdata
	public static int ubOfData = 1;
	
	//iteration_bound
	public static int boundOfIter = 50;
	
	
	// number of vms of datacenter
	public static int[] numberOfVMperDC = {200,200};
	
	// vm params
	
	public static int nVms;
	public static int taskSlotsPerVm = 1;

	public static double numberOfCusPerPe = 1;
	public static int numberOfPes = 1;
	public static int ram = (int) (1.7 * 1024);
	
	// uplink of datacenter
	public static double[] uplinkOfDC = {10000,10000};
	
	// downlink of datacenter
	public static double[] downlinkOfDC = {10000,10000};
	
	// the probability for a task to end in failure instead of success once it's
	// execution time has passed
	public static double[] likelihoodOfFailure = {0.002,0.002};
	public static double[] runtimeFactorInCaseOfFailure = {20d,20d};
	
	// the probability for a datacenter failure
	public static double[] likelihoodOfDCFailure = {0.2,0.0001};
	
	public static double[] ubOfDCFailureDuration = {50d,50d};
	public static double[] lbOfDCFailureDuration = {5d,5d};

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
	
	public static void printJobList(List<Job> list) {
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Job ID" + indent + "Task ID" + indent + "STATUS" + indent + indent
                + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");
        DecimalFormat dft = new DecimalFormat("###.##");
        for (Job job : list) {
        	
        	if(job.getStatus() == Cloudlet.SUCCESS) {
        		
        		Log.singleprint(indent + job.getCloudletId() + indent + indent);
                if (job.getClassType() == ClassType.STAGE_IN.value) {
                    Log.singleprint("Stage-in");
                }
                
                for (Task task : job.successTaskList) {
                    Log.singleprint(task.getCloudletId() + ",");
                }
                Log.singleprint(indent);

                if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                    Log.singleprint("SUCCESS");
                    Log.singleprintLine(indent + indent + indent + dft.format(job.getActualCPUTime())
                            + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                            + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
                } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                    Log.singleprint("FAILED");
                    Log.singleprintLine(indent + indent + indent + dft.format(job.getActualCPUTime())
                            + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                            + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
                }
        		
        	}
        }
    }
	
	public static void printJobListInput(List<Job> list) {
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== INPUT ==========");
        Log.printLine("Job ID" + indent + "Task ID" + indent + "STATUS" + indent + "Depth");
        DecimalFormat dft = new DecimalFormat("###.##");
        for (Job job : list) {	
    		Log.singleprint(indent + job.getCloudletId() + indent + indent);
            if (job.getClassType() == ClassType.STAGE_IN.value) {
                Log.singleprint("Stage-in");
            }
            
            for (Task task : job.getTaskList()) {
                Log.singleprint(task.getCloudletId() + ",");
            }
            Log.singleprint(indent);
            Log.singleprint(Cloudlet.getStatusString(job.getStatus()));
            Log.singleprintLine(indent + indent + indent + job.getDepth());
        }
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

	public static int sumOfVM() {
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
