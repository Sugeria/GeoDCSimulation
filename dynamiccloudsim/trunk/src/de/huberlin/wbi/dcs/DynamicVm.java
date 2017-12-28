package de.huberlin.wbi.dcs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.examples.Parameters.Distribution;

public class DynamicVm extends Vm {
	
	private long io;
	
	private double numberOfCusPerPe;
	
	private DynamicModel dynamicModel;
	
	private double currentMiCoefficient;
	private double currentIoCoefficient;
	private double currentBwCoefficient;
	
	private double previousTime;
	
	private BufferedWriter performanceLog;
	private int taskSlots;
	
	public DynamicVm(
			int id,
			int userId,
			double numberOfCusPerPe,
			int numberOfPes,
			int ram,
			long storage,
			String vmm,
			CloudletScheduler cloudletScheduler,
			DynamicModel dynamicModel,
			String performanceLogFileName,
			int taskSlots,
			int dcindex) {
		super(id, userId, -1, numberOfPes, ram, -1, storage, vmm, cloudletScheduler);
		setNumberOfCusPerPe(numberOfCusPerPe);
		setDynamicModel(dynamicModel);
		super.DCindex = dcindex;
		setCoefficients(
				Parameters.cpuNoiseDistributionOfDC[dcindex],
				Parameters.cpuNoiseCVOfDC[dcindex],
				Parameters.cpuNoiseAlphaOfDC[0],
				Parameters.cpuNoiseBetaOfDC[0],
				Parameters.cpuNoiseShapeOfDC[0],
				Parameters.cpuNoiseLocationOfDC[0],
				Parameters.cpuNoiseShiftOfDC[0],
				Parameters.cpuNoiseMinOfDC[0],
				Parameters.cpuNoiseMaxOfDC[0],
				Parameters.cpuNoisePopulationOfDC[0],
				Parameters.ioNoiseDistributionOfDC[dcindex],
				Parameters.ioNoiseCVOfDC[dcindex],
				Parameters.ioNoiseAlphaOfDC[0],
				Parameters.ioNoiseBetaOfDC[0],
				Parameters.ioNoiseShapeOfDC[0],
				Parameters.ioNoiseLocationOfDC[0],
				Parameters.ioNoiseShiftOfDC[0],
				Parameters.ioNoiseMinOfDC[0],
				Parameters.ioNoiseMaxOfDC[0],
				Parameters.ioNoisePopulationOfDC[0],
				Parameters.bwNoiseDistributionOfDC[dcindex],
				Parameters.bwNoiseCVOfDC[dcindex],
				Parameters.bwNoiseAlphaOfDC[0],
				Parameters.bwNoiseBetaOfDC[0],
				Parameters.bwNoiseShapeOfDC[0],
				Parameters.bwNoiseLocationOfDC[0],
				Parameters.bwNoiseShiftOfDC[0],
				Parameters.bwNoiseMinOfDC[0],
				Parameters.bwNoiseMaxOfDC[0],
				Parameters.bwNoisePopulationOfDC[0]
				);
		previousTime = CloudSim.clock();
		this.taskSlots = taskSlots;
		if (Parameters.outputVmPerformanceLogs) {
			try {
				performanceLog = new BufferedWriter(new FileWriter(new File(performanceLogFileName)));
				performanceLog.write("time");
				String[] resources = {"mips", "iops", "bwps"};
				for (String resource : resources) {
					for (int i = 0; i < taskSlots; i++) {
						performanceLog.write("," + resource + " task slot " + i);
					}
					performanceLog.write("," + resource + " unassigned");
				}
				performanceLog.write("\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (cloudletScheduler instanceof CloudletSchedulerGreedyDivided) {
			CloudletSchedulerGreedyDivided cloudletSchedulerGreedyDivided = (CloudletSchedulerGreedyDivided)cloudletScheduler;
			cloudletSchedulerGreedyDivided.setVm(this);
		}
	}
	
	public DynamicVm(
			int id,
			int userId,
			double numberOfCusPerPe,
			int numberOfPes,
			int ram,
			long storage,
			String vmm,
			CloudletScheduler cloudletScheduler,
			DynamicModel dynamicModel,
			String performanceLogFileName,
			int DCindex) {
		this(id, userId, numberOfCusPerPe, numberOfPes, ram, storage, vmm, cloudletScheduler, dynamicModel, performanceLogFileName, 1,DCindex);
	}
	
	public void updatePerformanceCoefficients () {
		double currentTime = CloudSim.clock();
		double timespan = currentTime - getPreviousTime();
		setPreviousTime(currentTime);
		int dcindex = DCindex;
		dynamicModel.updateBaselines(timespan,
				Parameters.cpuBaselineChangesPerHourOfDC[dcindex],
				Parameters.ioBaselineChangesPerHourOfDC[dcindex],
				Parameters.bwBaselineChangesPerHourOfDC[dcindex],
				Parameters.cpuDynamicsDistributionOfDC[dcindex],
				Parameters.cpuDynamicsCVOfDC[dcindex],
				Parameters.cpuDynamicsAlphaOfDC[0],
				Parameters.cpuDynamicsBetaOfDC[0],
				Parameters.cpuDynamicsShapeOfDC[0],
				Parameters.cpuDynamicsLocationOfDC[0],
				Parameters.cpuDynamicsShiftOfDC[0],
				Parameters.cpuDynamicsMinOfDC[0],
				Parameters.cpuDynamicsMaxOfDC[0],
				Parameters.cpuDynamicsPopulationOfDC[0],
				Parameters.ioDynamicsDistributionOfDC[dcindex],
				Parameters.ioDynamicsCVOfDC[dcindex],
				Parameters.ioDynamicsAlphaOfDC[0],
				Parameters.ioDynamicsBetaOfDC[0],
				Parameters.ioDynamicsShapeOfDC[0],
				Parameters.ioDynamicsLocationOfDC[0],
				Parameters.ioDynamicsShiftOfDC[0],
				Parameters.ioDynamicsMinOfDC[0],
				Parameters.ioDynamicsMaxOfDC[0],
				Parameters.ioDynamicsPopulationOfDC[0],
				Parameters.bwDynamicsDistributionOfDC[dcindex],
				Parameters.bwDynamicsCVOfDC[dcindex],
				Parameters.bwDynamicsAlphaOfDC[0],
				Parameters.bwDynamicsBetaOfDC[0],
				Parameters.bwDynamicsShapeOfDC[0],
				Parameters.bwDynamicsLocationOfDC[0],
				Parameters.bwDynamicsShiftOfDC[0],
				Parameters.bwDynamicsMinOfDC[0],
				Parameters.bwDynamicsMaxOfDC[0],
				Parameters.bwDynamicsPopulationOfDC[0]
				);
		setCoefficients(
				Parameters.cpuNoiseDistributionOfDC[dcindex],
				Parameters.cpuNoiseCVOfDC[dcindex],
				Parameters.cpuNoiseAlphaOfDC[0],
				Parameters.cpuNoiseBetaOfDC[0],
				Parameters.cpuNoiseShapeOfDC[0],
				Parameters.cpuNoiseLocationOfDC[0],
				Parameters.cpuNoiseShiftOfDC[0],
				Parameters.cpuNoiseMinOfDC[0],
				Parameters.cpuNoiseMaxOfDC[0],
				Parameters.cpuNoisePopulationOfDC[0],
				Parameters.ioNoiseDistributionOfDC[dcindex],
				Parameters.ioNoiseCVOfDC[dcindex],
				Parameters.ioNoiseAlphaOfDC[0],
				Parameters.ioNoiseBetaOfDC[0],
				Parameters.ioNoiseShapeOfDC[0],
				Parameters.ioNoiseLocationOfDC[0],
				Parameters.ioNoiseShiftOfDC[0],
				Parameters.ioNoiseMinOfDC[0],
				Parameters.ioNoiseMaxOfDC[0],
				Parameters.ioNoisePopulationOfDC[0],
				Parameters.bwNoiseDistributionOfDC[0],
				Parameters.bwNoiseCVOfDC[0],
				Parameters.bwNoiseAlphaOfDC[0],
				Parameters.bwNoiseBetaOfDC[0],
				Parameters.bwNoiseShapeOfDC[0],
				Parameters.bwNoiseLocationOfDC[0],
				Parameters.bwNoiseShiftOfDC[0],
				Parameters.bwNoiseMinOfDC[0],
				Parameters.bwNoiseMaxOfDC[0],
				Parameters.bwNoisePopulationOfDC[0]
				);
		
	}
	
	private void setCoefficients(
			Distribution cpuNoiseDistribution,
			double cpuNoiseCV,
			int cpuNoiseAlpha,
			double cpuNoiseBeta,
			double cpuNoiseShape,
			double cpuNoiseLocation,
			double cpuNoiseShift,
			double cpuNoiseMin,
			double cpuNoiseMax,
			int cpuNoisePopulation,
			Distribution ioNoiseDistribution,
			double ioNoiseCV,
			int ioNoiseAlpha,
			double ioNoiseBeta,
			double ioNoiseShape,
			double ioNoiseLocation,
			double ioNoiseShift,
			double ioNoiseMin,
			double ioNoiseMax,
			int ioNoisePopulation,
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
		setCurrentMiCoefficient(dynamicModel.nextMiCoefficient(
				cpuNoiseDistribution,
				cpuNoiseCV,
				cpuNoiseAlpha,
				cpuNoiseBeta,
				cpuNoiseShape,
				cpuNoiseLocation,
				cpuNoiseShift,
				cpuNoiseMin,
				cpuNoiseMax,
				cpuNoisePopulation
				));
		setCurrentIoCoefficient(dynamicModel.nextIoCoefficient(
				ioNoiseDistribution,
				ioNoiseCV,
				ioNoiseAlpha,
				ioNoiseBeta,
				ioNoiseShape,
				ioNoiseLocation,
				ioNoiseShift,
				ioNoiseMin,
				ioNoiseMax,
				ioNoisePopulation
				));
		setCurrentBwCoefficient(dynamicModel.nextBwCoefficient(
				bwNoiseDistribution,
				bwNoiseCV,
				bwNoiseAlpha,
				bwNoiseBeta,
				bwNoiseShape,
				bwNoiseLocation,
				bwNoiseShift,
				bwNoiseMin,
				bwNoiseMax,
				bwNoisePopulation
				));
	}
	
	public void setMips(double mips) {
		super.setMips(mips);
	}
	
	public long getIo() {
		return io;
	}
	
	public double getNumberOfCusPerPe() {
		return numberOfCusPerPe;
	}
	
	public void setIo(long io) {
		this.io = io;
	}
	
	public void setNumberOfCusPerPe(double numberOfCusPerPe) {
		this.numberOfCusPerPe = numberOfCusPerPe;
	}
	
	public DynamicModel getDynamicModel() {
		return dynamicModel;
	}
	
	public void setDynamicModel(DynamicModel dynamicModel) {
		this.dynamicModel = dynamicModel;
	}
	
	public double getCurrentBwCoefficient() {
		return currentBwCoefficient;
	}
	
	public double getCurrentIoCoefficient() {
		return currentIoCoefficient;
	}
	
	public double getCurrentMiCoefficient() {
		return currentMiCoefficient;
	}
	
	private void setCurrentBwCoefficient(double currentBwCoefficient) {
		this.currentBwCoefficient = currentBwCoefficient;
	}
	
	private void setCurrentIoCoefficient(double currentIoCoefficient) {
		this.currentIoCoefficient = currentIoCoefficient;
	}
	
	private void setCurrentMiCoefficient(double currentMiCoefficient) {
		this.currentMiCoefficient = currentMiCoefficient;
	}
	
	public double getPreviousTime() {
		return previousTime;
	}
	
	public void setPreviousTime(double previousTime) {
		this.previousTime = previousTime;
	}
	
	public BufferedWriter getPerformanceLog() {
		return performanceLog;
	}
	
	public void closePerformanceLog() {
		if (Parameters.outputVmPerformanceLogs) {
			try {
				performanceLog.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public int getTaskSlots() {
		return taskSlots;
	}
}
