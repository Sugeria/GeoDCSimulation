package de.huberlin.wbi.dcs.examples;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import javax.jws.soap.SOAPBinding.ParameterStyle;

import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import com.sun.org.apache.xalan.internal.xsltc.runtime.Parameter;

import EDU.oswego.cs.dl.util.concurrent.FJTask.Par;
import de.huberlin.wbi.cuneiform.core.semanticmodel.Param;
import de.huberlin.wbi.dcs.CloudletSchedulerGreedyDivided;
import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.DynamicModel;
import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.VmAllocationPolicyRandom;
import de.huberlin.wbi.dcs.examples.Parameters.Distribution;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;
import de.huberlin.wbi.dcs.workflow.io.AlignmentTraceFileReader;
import de.huberlin.wbi.dcs.workflow.io.CuneiformLogFileReader;
import de.huberlin.wbi.dcs.workflow.io.DaxFileReader;
import de.huberlin.wbi.dcs.workflow.io.MontageTraceFileReader;
import de.huberlin.wbi.dcs.workflow.scheduler.C2O;
import de.huberlin.wbi.dcs.workflow.scheduler.C3;
import de.huberlin.wbi.dcs.workflow.scheduler.GreedyQueueScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.HEFTScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.LATEScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.StaticRoundRobinScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.AbstractWorkflowScheduler;

public class WorkflowExample {

	public static void main(String[] args) {
		double totalRuntime = 0d;
		//Parameters.parseParameters(args);

		try {
			for (int i = 0; i < Parameters.numberOfRuns; i++) {
				WorkflowExample ex = new WorkflowExample();
				if (!Parameters.outputDatacenterEvents) {
					Log.disable();
				}
				// Initialize the CloudSim package
				int num_user = 1; // number of grid users
				Calendar calendar = Calendar.getInstance();
				boolean trace_flag = false; // mean trace events
				CloudSim.init(num_user, calendar, trace_flag);

				// ex.createDatacenter("Datacenter");
				ex.createMulDatacenters(Parameters.numberOfDC);
				AbstractWorkflowScheduler scheduler = ex.createScheduler(i);
				ex.createVms(i, scheduler);
				Workflow workflow = buildWorkflow(scheduler);
				ex.submitWorkflow(workflow, scheduler);

				// Start the simulation
				CloudSim.startSimulation();
				CloudSim.stopSimulation();

				totalRuntime += scheduler.getRuntime();
				Log.printLine(scheduler.getRuntime() / 60);
			}

			Log.printLine("Average runtime in minutes: " + totalRuntime
					/ Parameters.numberOfRuns / 60);
			Log.printLine("Total Workload: " + Task.getTotalMi() + "mi "
					+ Task.getTotalIo() + "io " + Task.getTotalBw() + "bw");
			Log.printLine("Total VM Performance: " + DynamicHost.getTotalMi()
					+ "mips " + DynamicHost.getTotalIo() + "iops "
					+ DynamicHost.getTotalBw() + "bwps");
			Log.printLine("minimum minutes (quotient): " + Task.getTotalMi()
					/ DynamicHost.getTotalMi() / 60 + " " + Task.getTotalIo()
					/ DynamicHost.getTotalIo() / 60 + " " + Task.getTotalBw()
					/ DynamicHost.getTotalBw() / 60);
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}

	}
	
	
	

	public AbstractWorkflowScheduler createScheduler(int i) {
		try {
			switch (Parameters.scheduler) {
			case STATIC_ROUND_ROBIN:
				return new StaticRoundRobinScheduler(
						"StaticRoundRobinScheduler", Parameters.taskSlotsPerVm);
			case LATE:
				return new LATEScheduler("LATEScheduler", Parameters.taskSlotsPerVm);
			case HEFT:
				return new HEFTScheduler("HEFTScheduler", Parameters.taskSlotsPerVm);
			case JOB_QUEUE:
				return new GreedyQueueScheduler("GreedyQueueScheduler",
						Parameters.taskSlotsPerVm);
			case C3:
				return new C3("C3", Parameters.taskSlotsPerVm);
			case C2O:
				return new C2O("C2O", Parameters.taskSlotsPerVm, i);
			default:
				return new GreedyQueueScheduler("GreedyQueueScheduler",
						Parameters.taskSlotsPerVm);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public void createVms(int run, AbstractWorkflowScheduler scheduler) {
		// Create VMs
		List<Vm> vmlist = createVMList(scheduler.getId(), run);
		scheduler.submitVmList(vmlist);
	}

	public static Workflow buildWorkflow(AbstractWorkflowScheduler scheduler) {
		switch (Parameters.experiment) {
		case MONTAGE_TRACE_1:
			return new MontageTraceFileReader().parseLogFile(scheduler.getId(),
					"examples/montage.m17.1.trace", true, true, ".*jpg");
		case MONTAGE_TRACE_12:
			return new MontageTraceFileReader().parseLogFile(scheduler.getId(),
					"examples/montage.m17.12.trace", true, true, ".*jpg");
		case ALIGNMENT_TRACE:
			return new AlignmentTraceFileReader().parseLogFile(
					scheduler.getId(), "examples/alignment.caco.geo.chr22.trace2", true,
					true, null);
		case MONTAGE_25:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Montage_25.xml", true, true, null);
		case MONTAGE_1000:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Montage_1000.xml", true, true, null);
		case CYBERSHAKE_1000:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CyberShake_1000.xml", true, true, null);
		case EPIGENOMICS_997:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Epigenomics_997.xml", true, true, null);
		case CUNEIFORM_VARIANT_CALL:
			return new CuneiformLogFileReader().parseLogFile(scheduler.getId(),
					"examples/i1_s11756_r7_greedyQueue.log", true, true, null);
		case HETEROGENEOUS_TEST_WORKFLOW:
			return new CuneiformLogFileReader().parseLogFile(scheduler.getId(),
					"examples/heterogeneous_test_workflow.log", true, true, null);
		}
		return null;
	}

	public void submitWorkflow(Workflow workflow, AbstractWorkflowScheduler scheduler) {
		// Create Cloudlets and send them to Scheduler
		if (Parameters.outputWorkflowGraph) {
			workflow.visualize(1920, 1200);
		}
		scheduler.submitWorkflow(workflow);
	}

	
	public void createMulDatacenters(int numberOfDC) {
		StringBuilder sb = new StringBuilder("Datacenter_");
		Parameters Para = new Parameters();
		for (int dcindex = 0;dcindex < numberOfDC; dcindex++) {
			StringBuilder dcname = sb;
			dcname.append(String.valueOf(dcindex));
			Para.setLikelihoodOfStraggler(Parameters.likelihoodOfStragglerOfDC[dcindex]);
			Para.setStragglerPerformanceCoefficient(Parameters.stragglerPerformanceCoefficientOfDC[dcindex]);
			
			Para.setDCHeterogeneity(
					Parameters.cpuHeterogeneityDistributionOfDC[dcindex],
					Parameters.cpuHeterogeneityCVOfDC[dcindex],
					Parameters.cpuHeterogeneityAlphaOfDC[dcindex],
					Parameters.cpuHeterogeneityBetaOfDC[dcindex],
					Parameters.cpuHeterogeneityShapeOfDC[dcindex],
					Parameters.cpuHeterogeneityLocationOfDC[dcindex],
					Parameters.cpuHeterogeneityShiftOfDC[dcindex],
					Parameters.cpuHeterogeneityMinOfDC[dcindex],
					Parameters.cpuHeterogeneityMaxOfDC[dcindex],
					Parameters.cpuHeterogeneityPopulationOfDC[dcindex],
					Parameters.ioHeterogeneityDistributionOfDC[dcindex],
					Parameters.ioHeterogeneityCVOfDC[dcindex],
					Parameters.ioHeterogeneityAlphaOfDC[dcindex],
					Parameters.ioHeterogeneityBetaOfDC[dcindex],
					Parameters.ioHeterogeneityShapeOfDC[dcindex],
					Parameters.ioHeterogeneityLocationOfDC[dcindex],
					Parameters.ioHeterogeneityShiftOfDC[dcindex],
					Parameters.ioHeterogeneityMinOfDC[dcindex],
					Parameters.ioHeterogeneityMaxOfDC[dcindex],
					Parameters.ioHeterogeneityPopulationOfDC[dcindex],
					Parameters.bwHeterogeneityDistributionOfDC[dcindex],
					Parameters.bwHeterogeneityCVOfDC[dcindex],
					Parameters.bwHeterogeneityAlphaOfDC[dcindex],
					Parameters.bwHeterogeneityBetaOfDC[dcindex],
					Parameters.bwHeterogeneityShapeOfDC[dcindex],
					Parameters.bwHeterogeneityLocationOfDC[dcindex],
					Parameters.bwHeterogeneityShiftOfDC[dcindex],
					Parameters.bwHeterogeneityMinOfDC[dcindex],
					Parameters.bwHeterogeneityMaxOfDC[dcindex],
					Parameters.bwHeterogeneityPopulationOfDC[dcindex],
					Parameters.nOpteronOfMachineTypeOfDC[dcindex]
					);
			Datacenter dc = createDatacenter(dcname.toString());
			dc.setDownlink(Parameters.downlinkOfDC[numberOfDC]);
			dc.setUplink(Parameters.uplinkOfDC[numberOfDC]);
			DatacenterCharacteristics dcc = dc.getCharacteristics();
			dcc.setLikelihoodOfFailure(Parameters.likelihoodOfFailure[numberOfDC]);
			dcc.setRuntimeFactorIncaseOfFailure(Parameters.runtimeFactorInCaseOfFailure[numberOfDC]);
			dcc.setLikelihoodOfDCFailure(Parameters.likelihoodOfDCFailure[numberOfDC]);
			// CPU Dynamics
			dcc.cpuBaselineChangesPerHour = Parameters.cpuBaselineChangesPerHourOfDC[dcindex];
			dcc.cpuDynamicsDistribution = Parameters.cpuDynamicsDistributionOfDC[dcindex];
			dcc.cpuDynamicsCV = Parameters.cpuDynamicsCVOfDC[dcindex];
			dcc.cpuDynamicsAlpha = Parameters.cpuDynamicsAlphaOfDC[dcindex];
			dcc.cpuDynamicsBeta = Parameters.cpuDynamicsBetaOfDC[dcindex];
			dcc.cpuDynamicsShape = Parameters.cpuDynamicsShapeOfDC[dcindex];
			dcc.cpuDynamicsLocation = Parameters.cpuDynamicsLocationOfDC[dcindex];
			dcc.cpuDynamicsShift = Parameters.cpuDynamicsShiftOfDC[dcindex];
			dcc.cpuDynamicsMin = Parameters.cpuDynamicsMinOfDC[dcindex];
			dcc.cpuDynamicsMax = Parameters.cpuDynamicsMaxOfDC[dcindex];
			dcc.cpuDynamicsPopulation = Parameters.cpuDynamicsPopulationOfDC[dcindex];

			// IO Dynamics
			dcc.ioBaselineChangesPerHour = Parameters.ioBaselineChangesPerHourOfDC[dcindex];
			dcc.ioDynamicsDistribution = Parameters.ioDynamicsDistributionOfDC[dcindex];
			dcc.ioDynamicsCV = Parameters.ioDynamicsCVOfDC[dcindex];
			dcc.ioDynamicsAlpha = Parameters.ioDynamicsAlphaOfDC[dcindex];
			dcc.ioDynamicsBeta = Parameters.ioDynamicsBetaOfDC[dcindex];
			dcc.ioDynamicsShape = Parameters.ioDynamicsShapeOfDC[dcindex];
			dcc.ioDynamicsLocation = Parameters.ioDynamicsLocationOfDC[dcindex];
			dcc.ioDynamicsShift = Parameters.ioDynamicsShiftOfDC[dcindex];
			dcc.ioDynamicsMin = Parameters.ioDynamicsMinOfDC[dcindex];
			dcc.ioDynamicsMax = Parameters.ioDynamicsMaxOfDC[dcindex];
			dcc.ioDynamicsPopulation = Parameters.ioDynamicsPopulationOfDC[dcindex];

			// BW Dynamics
			dcc.bwBaselineChangesPerHour = Parameters.bwBaselineChangesPerHourOfDC[dcindex];
			dcc.bwDynamicsDistribution = Parameters.bwDynamicsDistributionOfDC[dcindex];
			dcc.bwDynamicsCV = Parameters.bwDynamicsCVOfDC[dcindex];
			dcc.bwDynamicsAlpha = Parameters.bwDynamicsAlphaOfDC[dcindex];
			dcc.bwDynamicsBeta = Parameters.bwDynamicsBetaOfDC[dcindex];
			dcc.bwDynamicsShape = Parameters.bwDynamicsShapeOfDC[dcindex];
			dcc.bwDynamicsLocation = Parameters.bwDynamicsLocationOfDC[dcindex];
			dcc.bwDynamicsShift = Parameters.bwDynamicsShiftOfDC[dcindex];
			dcc.bwDynamicsMin = Parameters.bwDynamicsMinOfDC[dcindex];
			dcc.bwDynamicsMax = Parameters.bwDynamicsMaxOfDC[dcindex];
			dcc.bwDynamicsPopulation = Parameters.bwDynamicsPopulationOfDC[dcindex];

			// CPU noise
			dcc.cpuNoiseDistribution = Parameters.cpuNoiseDistributionOfDC[dcindex];
			dcc.cpuNoiseCV = Parameters.cpuNoiseCVOfDC[dcindex];
			dcc.cpuNoiseAlpha = Parameters.cpuNoiseAlphaOfDC[dcindex];
			dcc.cpuNoiseBeta = Parameters.cpuNoiseBetaOfDC[dcindex];
			dcc.cpuNoiseShape = Parameters.cpuNoiseShapeOfDC[dcindex];
			dcc.cpuNoiseLocation = Parameters.cpuNoiseLocationOfDC[dcindex];
			dcc.cpuNoiseShift = Parameters.cpuNoiseShiftOfDC[dcindex];
			dcc.cpuNoiseMin = Parameters.cpuNoiseMinOfDC[dcindex];
			dcc.cpuNoiseMax = Parameters.cpuNoiseMaxOfDC[dcindex];
			dcc.cpuNoisePopulation = Parameters.cpuNoisePopulationOfDC[dcindex];

			// IO noise
			dcc.ioNoiseDistribution = Parameters.ioNoiseDistributionOfDC[dcindex];
			dcc.ioNoiseCV = Parameters.ioNoiseCVOfDC[dcindex];
			dcc.ioNoiseAlpha = Parameters.ioNoiseAlphaOfDC[dcindex];
			dcc.ioNoiseBeta = Parameters.ioNoiseBetaOfDC[dcindex];
			dcc.ioNoiseShape = Parameters.ioNoiseShapeOfDC[dcindex];
			dcc.ioNoiseLocation = Parameters.ioNoiseLocationOfDC[dcindex];
			dcc.ioNoiseShift = Parameters.ioNoiseShiftOfDC[dcindex];
			dcc.ioNoiseMin = Parameters.ioNoiseMinOfDC[dcindex];
			dcc.ioNoiseMax = Parameters.ioNoiseMaxOfDC[dcindex];
			dcc.ioNoisePopulation = Parameters.ioNoisePopulationOfDC[dcindex];

			// BW noise
			dcc.bwNoiseDistribution = Parameters.bwNoiseDistributionOfDC[dcindex];
			dcc.bwNoiseCV = Parameters.bwNoiseCVOfDC[dcindex];
			dcc.bwNoiseAlpha = Parameters.bwNoiseAlphaOfDC[dcindex];
			dcc.bwNoiseBeta = Parameters.bwNoiseBetaOfDC[dcindex];
			dcc.bwNoiseShape = Parameters.bwNoiseShapeOfDC[dcindex];
			dcc.bwNoiseLocation = Parameters.bwNoiseLocationOfDC[dcindex];
			dcc.bwNoiseShift = Parameters.bwNoiseShiftOfDC[dcindex];
			dcc.bwNoiseMin = Parameters.bwNoiseMinOfDC[dcindex];
			dcc.bwNoiseMax = Parameters.bwNoiseMaxOfDC[dcindex];
			dcc.bwNoisePopulation = Parameters.bwNoisePopulationOfDC[dcindex];
			
			dcc.MIPSbaseline = Parameters.MIPSbaselineOfDC[dcindex];
			dcc.bwBaseline = Parameters.bwBaselineOfDC[dcindex];
			dcc.ioBaseline = Parameters.ioBaselineOfDC[dcindex];
		}
	}
	
	
	
	// all numbers in 1000 (e.g. kb/s)
	public Datacenter createDatacenter(String name) {
		Random numGen;
		Parameters parameters = new Parameters();
		numGen = Parameters.numGen;
		List<DynamicHost> hostList = new ArrayList<DynamicHost>();
		int hostId = 0;
		long storage = 1024 * 1024;

		for(int typeindex = 0; typeindex < Parameters.machineType; typeindex++) {
			int ram = (int) (2 * 1024 * Parameters.nCusPerCoreOpteronOfMachineType[typeindex] * Parameters.nCoresOpteronOfMachineType[typeindex]);
			for (int i = 0; i < parameters.nOpteronOfMachineType[typeindex]; i++) {
				double mean = 1d;
				double dev = parameters.bwHeterogeneityCV;
				ContinuousDistribution dist = Parameters.getDistribution(
						parameters.bwHeterogeneityDistribution, mean,
						parameters.bwHeterogeneityAlpha,
						parameters.bwHeterogeneityBeta, dev,
						parameters.bwHeterogeneityShape,
						parameters.bwHeterogeneityLocation,
						parameters.bwHeterogeneityShift,
						parameters.bwHeterogeneityMin,
						parameters.bwHeterogeneityMax,
						parameters.bwHeterogeneityPopulation);
				long bwps = 0;
				while (bwps <= 0) {
					bwps = (long) (dist.sample() * Parameters.bwpsPerPeOfMachineType[typeindex]);
				}
				mean = 1d;
				dev = parameters.ioHeterogeneityCV;
				dist = Parameters.getDistribution(
						parameters.ioHeterogeneityDistribution, mean,
						parameters.ioHeterogeneityAlpha,
						parameters.ioHeterogeneityBeta, dev,
						parameters.ioHeterogeneityShape,
						parameters.ioHeterogeneityLocation,
						parameters.ioHeterogeneityShift,
						parameters.ioHeterogeneityMin,
						parameters.ioHeterogeneityMax,
						parameters.ioHeterogeneityPopulation);
				long iops = 0;
				while (iops <= 0) {
					iops = (long) (long) (dist.sample() * Parameters.iopsPerPeOfMachineType[typeindex]);
				}
				mean = 1d;
				dev = parameters.cpuHeterogeneityCV;
				dist = Parameters.getDistribution(
						parameters.cpuHeterogeneityDistribution, mean,
						parameters.cpuHeterogeneityAlpha,
						parameters.cpuHeterogeneityBeta, dev,
						parameters.cpuHeterogeneityShape,
						parameters.cpuHeterogeneityLocation,
						parameters.cpuHeterogeneityShift,
						parameters.cpuHeterogeneityMin,
						parameters.cpuHeterogeneityMax,
						parameters.cpuHeterogeneityPopulation);
				long mips = 0;
				while (mips <= 0) {
					mips = (long) (long) (dist.sample() * Parameters.mipsPerCoreOpteronOfMachineType[typeindex]);
				}
				if (numGen.nextDouble() < parameters.likelihoodOfStraggler) {
					bwps *= parameters.stragglerPerformanceCoefficient;
					iops *= parameters.stragglerPerformanceCoefficient;
					mips *= parameters.stragglerPerformanceCoefficient;
				}
				hostList.add(new DynamicHost(hostId++, ram, bwps, iops, storage,
						Parameters.nCusPerCoreOpteronOfMachineType[typeindex], Parameters.nCoresOpteronOfMachineType[typeindex], mips));
			}
		}
		
		
		String arch = "x86";
		String os = "Linux";
		String vmm = "Xen";
		double time_zone = 10.0;
		double cost = 3.0;
		double costPerMem = 0.05;
		double costPerStorage = 0.001;
		double costPerBw = 0.0;
		LinkedList<Storage> storageList = new LinkedList<Storage>();

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem,
				costPerStorage, costPerBw);

		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics,
					new VmAllocationPolicyRandom(hostList, Parameters.seed++),
					storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	public List<Vm> createVMList(int userId, int run) {

		// Creates a container to store VMs. This list is passed to the broker
		// later
		LinkedList<Vm> list = new LinkedList<Vm>();

		// VM Parameters
		long storage = 10000;
		String vmm = "Xen";

		// create VMs
		Vm[] vm = new DynamicVm[Parameters.nVms];
		int vmnum = 0;
		for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
			for (int j = 0; j < Parameters.numberOfVMperDC[dcindex]; j++) {
				DynamicModel dynamicModel = new DynamicModel(
						Parameters.cpuDynamicsDistributionOfDC[dcindex],
						Parameters.cpuDynamicsCVOfDC[dcindex],
						Parameters.cpuDynamicsAlphaOfDC[dcindex],
						Parameters.cpuDynamicsBetaOfDC[dcindex],
						Parameters.cpuDynamicsShapeOfDC[dcindex],
						Parameters.cpuDynamicsLocationOfDC[dcindex],
						Parameters.cpuDynamicsShiftOfDC[dcindex],
						Parameters.cpuDynamicsMinOfDC[dcindex],
						Parameters.cpuDynamicsMaxOfDC[dcindex],
						Parameters.cpuDynamicsPopulationOfDC[dcindex],
						Parameters.ioDynamicsDistributionOfDC[dcindex],
						Parameters.ioDynamicsCVOfDC[dcindex],
						Parameters.ioDynamicsAlphaOfDC[dcindex],
						Parameters.ioDynamicsBetaOfDC[dcindex],
						Parameters.ioDynamicsShapeOfDC[dcindex],
						Parameters.ioDynamicsLocationOfDC[dcindex],
						Parameters.ioDynamicsShiftOfDC[dcindex],
						Parameters.ioDynamicsMinOfDC[dcindex],
						Parameters.ioDynamicsMaxOfDC[dcindex],
						Parameters.ioDynamicsPopulationOfDC[dcindex],
						Parameters.bwDynamicsDistributionOfDC[dcindex],
						Parameters.bwDynamicsCVOfDC[dcindex],
						Parameters.bwDynamicsAlphaOfDC[dcindex],
						Parameters.bwDynamicsBetaOfDC[dcindex],
						Parameters.bwDynamicsShapeOfDC[dcindex],
						Parameters.bwDynamicsLocationOfDC[dcindex],
						Parameters.bwDynamicsShiftOfDC[dcindex],
						Parameters.bwDynamicsMinOfDC[dcindex],
						Parameters.bwDynamicsMaxOfDC[dcindex],
						Parameters.bwDynamicsPopulationOfDC[dcindex]
						);
				vm[vmnum] = new DynamicVm(vmnum, userId, Parameters.numberOfCusPerPe, Parameters.numberOfPes,
						Parameters.ram, storage, vmm, new CloudletSchedulerGreedyDivided(),
						dynamicModel, "output/run_" + run + "_vm_" + vmnum + ".csv",
						Parameters.taskSlotsPerVm,dcindex);
				list.add(vm[vmnum]);
			}
		}

		return list;
	}

}
