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
		super.DCindex = DCindex;
		setCoefficients(
				Parameters.cpuNoiseDistributionOfDC[dcindex],
				Parameters.cpuNoiseCVOfDC[dcindex],
				Parameters.cpuNoiseAlphaOfDC[dcindex],
				Parameters.cpuNoiseBetaOfDC[dcindex],
				Parameters.cpuNoiseShapeOfDC[dcindex],
				Parameters.cpuNoiseLocationOfDC[dcindex],
				Parameters.cpuNoiseShiftOfDC[dcindex],
				Parameters.cpuNoiseMinOfDC[dcindex],
				Parameters.cpuNoiseMaxOfDC[dcindex],
				Parameters.cpuNoisePopulationOfDC[dcindex],
				Parameters.ioNoiseDistributionOfDC[dcindex],
				Parameters.ioNoiseCVOfDC[dcindex],
				Parameters.ioNoiseAlphaOfDC[dcindex],
				Parameters.ioNoiseBetaOfDC[dcindex],
				Parameters.ioNoiseShapeOfDC[dcindex],
				Parameters.ioNoiseLocationOfDC[dcindex],
				Parameters.ioNoiseShiftOfDC[dcindex],
				Parameters.ioNoiseMinOfDC[dcindex],
				Parameters.ioNoiseMaxOfDC[dcindex],
				Parameters.ioNoisePopulationOfDC[dcindex],
				Parameters.bwNoiseDistributionOfDC[dcindex],
				Parameters.bwNoiseCVOfDC[dcindex],
				Parameters.bwNoiseAlphaOfDC[dcindex],
				Parameters.bwNoiseBetaOfDC[dcindex],
				Parameters.bwNoiseShapeOfDC[dcindex],
				Parameters.bwNoiseLocationOfDC[dcindex],
				Parameters.bwNoiseShiftOfDC[dcindex],
				Parameters.bwNoiseMinOfDC[dcindex],
				Parameters.bwNoiseMaxOfDC[dcindex],
				Parameters.bwNoisePopulationOfDC[dcindex]
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
		setCoefficients(
				Parameters.cpuNoiseDistributionOfDC[dcindex],
				Parameters.cpuNoiseCVOfDC[dcindex],
				Parameters.cpuNoiseAlphaOfDC[dcindex],
				Parameters.cpuNoiseBetaOfDC[dcindex],
				Parameters.cpuNoiseShapeOfDC[dcindex],
				Parameters.cpuNoiseLocationOfDC[dcindex],
				Parameters.cpuNoiseShiftOfDC[dcindex],
				Parameters.cpuNoiseMinOfDC[dcindex],
				Parameters.cpuNoiseMaxOfDC[dcindex],
				Parameters.cpuNoisePopulationOfDC[dcindex],
				Parameters.ioNoiseDistributionOfDC[dcindex],
				Parameters.ioNoiseCVOfDC[dcindex],
				Parameters.ioNoiseAlphaOfDC[dcindex],
				Parameters.ioNoiseBetaOfDC[dcindex],
				Parameters.ioNoiseShapeOfDC[dcindex],
				Parameters.ioNoiseLocationOfDC[dcindex],
				Parameters.ioNoiseShiftOfDC[dcindex],
				Parameters.ioNoiseMinOfDC[dcindex],
				Parameters.ioNoiseMaxOfDC[dcindex],
				Parameters.ioNoisePopulationOfDC[dcindex],
				Parameters.bwNoiseDistributionOfDC[dcindex],
				Parameters.bwNoiseCVOfDC[dcindex],
				Parameters.bwNoiseAlphaOfDC[dcindex],
				Parameters.bwNoiseBetaOfDC[dcindex],
				Parameters.bwNoiseShapeOfDC[dcindex],
				Parameters.bwNoiseLocationOfDC[dcindex],
				Parameters.bwNoiseShiftOfDC[dcindex],
				Parameters.bwNoiseMinOfDC[dcindex],
				Parameters.bwNoiseMaxOfDC[dcindex],
				Parameters.bwNoisePopulationOfDC[dcindex]
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
