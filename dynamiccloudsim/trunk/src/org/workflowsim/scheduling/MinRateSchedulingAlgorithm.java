package org.workflowsim.scheduling;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.Vm;
import org.workflowsim.Job;
import org.workflowsim.WorkflowScheduler;

import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWComplexity;
import com.mathworks.toolbox.javabuilder.MWFunctionHandle;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import com.sun.org.apache.bcel.internal.generic.NEW;

import de.huberlin.wbi.cuneiform.core.semanticmodel.Param;
import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.Task;
import ilog.concert.IloException;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;
import ilog.concert.IloRange;
import ilog.cplex.IloCplex;

public class MinRateSchedulingAlgorithm extends BaseSchedulingAlgorithm{
	
	public Map<Integer,Map<Integer,List<MWFunctionHandle>>> DCpdfOftasksInJob;
	
	public MinRateSchedulingAlgorithm() {
        super();
        DCpdfOftasksInJob = new HashMap<>();
    }
    private final List<Boolean> hasChecked = new ArrayList<>();

    
    @Override
    public void run() {

        int size = getCloudletList().size();
        hasChecked.clear();
        for (int t = 0; t < size; t++) {
            hasChecked.add(false);
        }
        
        
        // compute the optimal rate for the current job
        for(int jobindex = 0; jobindex < size; jobindex++) {
        	Job job = (Job)getCloudletList().get(jobindex);
        	if(job.getCloudletId()==50) {
        		double temp = 1d;
        	}
        	int jobId = job.getCloudletId();
        	List<Task> tasklist = job.unscheduledTaskList;
        	int numberOfTask = tasklist.size();
        	int vnumplusone = 1 + tasklist.size()*Parameters.numberOfDC;
        	int vnum = tasklist.size()*Parameters.numberOfDC;
        	double[] muParaOfTaskInDC = new double[vnum];
        	double[] sigmaParaOfTaskInDC = new double[vnum];
        	// 
        	int[] uselessDCforTask = new int[vnum];
        	for(int index = 0; index < uselessDCforTask.length; index++) {
        		uselessDCforTask[index] = -1;
        	}

        	
        	
        	
        	IloCplex cplex = null;
        	// use the MIT solver
        	try {
				cplex = new IloCplex();
				
				IloNumVar[] var = null;
				
				// up low datatype
				double[] xlb = new double[vnumplusone];
				double[] xub = new double[vnumplusone];
				IloNumVarType[] xTypes = new IloNumVarType[vnumplusone];
				for(int vindex = 0; vindex < vnumplusone; vindex++) {
					if(vindex == (vnumplusone - 1)) {
						xlb[vindex] = 0.0d;
						xub[vindex] = Double.MAX_VALUE;
						xTypes[vindex] = IloNumVarType.Float;
					}else {
						xlb[vindex] = 0.0;
						xub[vindex] = 1.0;
						xTypes[vindex] = IloNumVarType.Int;
					}
					
				}
				var = cplex.numVarArray(vnumplusone, xlb, xub,xTypes);
				
				// objective Function
				double[] objvals = new double[vnumplusone];
				for(int vindex = 0; vindex < vnumplusone; vindex++) {
					if(vindex == (vnum)) {
						objvals[vindex] = 1;
					}else {
						objvals[vindex] = 0;
					}
					
				}
				cplex.addMaximize(cplex.scalProd(var, objvals));
				
				double[][] probArray = new double[Parameters.numberOfDC][4];
				double[] unstablecoOfDC = new double[Parameters.numberOfDC];
				int[] data = new int[numberOfTask];
				double[][] datapos = new double[numberOfTask][Parameters.ubOfData];
				double[][] bandwidth = new double[vnum][Parameters.ubOfData];
				double[][] bandwidth_dataDelayOfTaskInDC = new double[1][vnum];
				double[][] bandwidth_dataDelay_co = new double[vnum][Parameters.ubOfData];
				double[][] SlotArray = new double[1][Parameters.numberOfDC];
				double[][] UpArray = new double[1][Parameters.numberOfDC];
				double[][] DownArray = new double[1][Parameters.numberOfDC];
				double[][] allRateMuArray = new double[1][vnum];
				double[][] allRateSigmaArray = new double[1][vnum];
				int uselessConstraintsNum = 0;
				//probArray
				for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
					for (int iterm = 0; iterm < 4; iterm++) {
						double value = 0d;
						switch (iterm) {
						case 0:
							value = (1-Parameters.likelihoodOfDCFailure[dcindex])*(1-Parameters.likelihoodOfFailure[dcindex])*(1-Parameters.likelihoodOfStragglerOfDC[dcindex]);
							break;
						case 1:
							value = (1-Parameters.likelihoodOfDCFailure[dcindex])*(1-Parameters.likelihoodOfFailure[dcindex])*Parameters.likelihoodOfStragglerOfDC[dcindex];
							break;
						case 2:
							value = (1-Parameters.likelihoodOfDCFailure[dcindex])*Parameters.likelihoodOfFailure[dcindex];
							break;
						case 3:
							value = Parameters.likelihoodOfDCFailure[dcindex];
							break;
						default:
							break;
						}
						probArray[dcindex][iterm] = value;
					}
				}
				
				//data datapos 
				double[] Totaldatasize = new double[numberOfTask];
				double[][] datasize = new double[numberOfTask][Parameters.ubOfData];
				double[] bestRateOfTask = new double[numberOfTask];
				
				
				for (int tindex = 0; tindex < numberOfTask; tindex++) {
					Task task = tasklist.get(tindex);
					data[tindex] = task.numberOfData;
					Totaldatasize[tindex] = 0d;
					
					for(int dataindex = 0; dataindex < task.numberOfData; dataindex++) {
						datapos[tindex][dataindex] = task.positionOfData[dataindex];
						task.positionOfDataID[dataindex] = task.positionOfData[dataindex] + DCbase;
						datasize[tindex][dataindex] = task.sizeOfData[dataindex];
						Totaldatasize[tindex] += task.sizeOfData[dataindex];
					}
					tasklist.set(tindex, task);
				}
				
				double[] TotalTransferDataSize = new double[numberOfTask*Parameters.numberOfDC];
				double[][] transferDataSize = new double[numberOfTask*Parameters.numberOfDC][Parameters.ubOfData];
				// bandwidth 
				for (int tindex = 0; tindex < numberOfTask; tindex++) {
					Task task = tasklist.get(tindex);
					for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
						double bw_mu = Parameters.bwBaselineOfDC[dcindex];
	        			
						int xindex = tindex*Parameters.numberOfDC + dcindex;
						int numberOfTransferData = 0;
						int datanumber = (int)data[tindex];
						double[] datasizeOfTask = new double[datanumber];
						for(int dataindex = 0; dataindex < datanumber; dataindex++) {
							datasizeOfTask[dataindex] = datasize[tindex][dataindex];
						}
						double TotaldatasizeOfTask = Totaldatasize[tindex];
						for(int dataindex = 0; dataindex < datanumber; dataindex++) {
							if (datapos[tindex][dataindex] == dcindex) {
								TotaldatasizeOfTask -= datasizeOfTask[dataindex];
								datasizeOfTask[dataindex] = 0;
							}else {
								numberOfTransferData++;
							}
						} 
						if(datanumber > 0) {
							task.numberOfTransferData[dcindex] = numberOfTransferData;
						}else {
							task.numberOfTransferData[dcindex] = 0;
						}
						
						TotalTransferDataSize[xindex] = TotaldatasizeOfTask;
						task.TotalTransferDataSize[dcindex] = TotaldatasizeOfTask;
						for(int dataindex = 0; dataindex < datanumber; dataindex++) {
							transferDataSize[xindex][dataindex] = datasizeOfTask[dataindex];
							task.transferDataSize[dcindex][dataindex] = datasizeOfTask[dataindex];
						}
						
						
						for(int dataindex = 0; dataindex < datanumber; dataindex++) {
							
							if (datasizeOfTask[dataindex] > 0) {
								bandwidth[xindex][dataindex] = bw_mu*datasizeOfTask[dataindex]/TotaldatasizeOfTask;
								task.bandwidth[dcindex][dataindex] = bandwidth[xindex][dataindex];
							}else {
								bandwidth[xindex][dataindex] = 0;
								task.bandwidth[dcindex][dataindex] = 0;
							}
						}
						
					}	
					tasklist.set(tindex, task);
					
				}
				
				Map<Integer, HashMap<Integer, Double>> objParaOfTaskInDC = new HashMap<>();
				
				for(int tindex = 0; tindex < numberOfTask; tindex++) {
					Task task = tasklist.get(tindex);
					int taskId = task.getCloudletId();
					objParaOfTaskInDC.put(taskId, new HashMap<>());
				}
				
				WorkflowScheduler scheduler = (WorkflowScheduler)workflowScheduler;
				// allRateMuArray allRateSigmaArray
//				for(int taskindex = 0; taskindex < numberOfTask; taskindex++) {
//					Task task = tasklist.get(taskindex);
//					double assignedDCBwExp = bandwidth_dataDelayOfTaskInDC[0][taskindex*Parameters.numberOfDC+task.submitDCIndex];
//					double mu = Parameters.MIPSbaselineOfDC[task.submitDCIndex]
//	        				+Parameters.ioBaselineOfDC[task.submitDCIndex]
//	        				+assignedDCBwExp;
//	        		double sigma = Math.sqrt(Math.pow(Parameters.MIPSbaselineOfDC[task.submitDCIndex]*Parameters.bwHeterogeneityCVOfDC[task.submitDCIndex], 2)
//	        				+Math.pow(Parameters.ioBaselineOfDC[task.submitDCIndex]*Parameters.ioHeterogeneityCVOfDC[task.submitDCIndex], 2)
//	        				+Math.pow(assignedDCBwExp*Parameters.bwHeterogeneityCVOfDC[task.submitDCIndex], 2));
//	        		
//	        		
//							
//					for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
//						int xindex = taskindex*Parameters.numberOfDC + dcindex;
//						double mi_mu = Parameters.MIPSbaselineOfDC[dcindex];
//	        			double mi_sigma = Parameters.MIPSbaselineOfDC[dcindex]*Parameters.bwHeterogeneityCVOfDC[dcindex];
//	        			double io_mu = Parameters.ioBaselineOfDC[dcindex];
//	        			double io_sigma = Parameters.ioBaselineOfDC[dcindex]*Parameters.ioHeterogeneityCVOfDC[dcindex];
//	        			double bw_mu_dataDelay = bandwidth_dataDelayOfTaskInDC[0][xindex];
//	        			double bw_co = Parameters.bwHeterogeneityCVOfDC[dcindex];
//	        			
//	        			// multiply unstable coefficient
//	        			double unstable_co = probArray[dcindex][0]
//	        					+ probArray[dcindex][1]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex]
//	        					+ probArray[dcindex][1]/Parameters.runtimeFactorInCaseOfFailure[dcindex];
//						muParaOfTaskInDC[xindex] = (mi_mu + io_mu + bw_mu_dataDelay) * unstable_co;
//	        			sigmaParaOfTaskInDC[xindex] = unstable_co * Math.sqrt(Math.pow(mi_sigma, 2)
//	        					+Math.pow(io_sigma, 2)+Math.pow((bw_mu_dataDelay*bw_co), 2));
//	        			double delay_para = (double)Parameters.delayAmongDCIndex[task.submitDCIndex][dcindex];
//	        			// the distance is too far
////	        			if((task.getCloudletLength()/(mu+sigma) - task.getCloudletLength()/(
////	        					muParaOfTaskInDC[xindex]+
////	        					sigmaParaOfTaskInDC[xindex])) < delay_para) {
////	        				uselessDCforTask[xindex] = 0;
////	        				uselessConstraintsNum += 1;
////	        			}
//	        			// in the submittedDC do not used the sigma
//	        			// compare with the potential interest executing in the other DC
////			        	if((task.getCloudletLength()/(mu) - task.getCloudletLength()/(
////		    					muParaOfTaskInDC[xindex]+
////		    					sigmaParaOfTaskInDC[xindex])) < delay_para) {
////		    				uselessDCforTask[xindex] = 0;
////		    				uselessConstraintsNum += 1;
////		    			}
//	        			
//	        			
//	        			// multiply taskinfo transfer coefficient
//	        			// just transfer the needed data
//	        			double task_workload = task.getMi() + task.getIo() + TotalTransferDataSize[xindex];
//	        			double delay_co = task_workload/(task_workload + muParaOfTaskInDC[xindex] * delay_para);
//	        			allRateMuArray[0][xindex] = muParaOfTaskInDC[xindex] * delay_co;
//	        			allRateSigmaArray[0][xindex] = sigmaParaOfTaskInDC[xindex] * delay_co;
//	        			objParaOfTaskInDC.get(task.getCloudletId()).put(dcindex, 
//	        					allRateMuArray[0][xindex]
//	        					+ Parameters.r * allRateSigmaArray[0][xindex]);
//					}
//				}
				
				// unstablecoOfDC
				for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
					double unstable_co = probArray[dcindex][0]
        					+ probArray[dcindex][1]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex]
        					+ probArray[dcindex][1]/Parameters.runtimeFactorInCaseOfFailure[dcindex];
					unstablecoOfDC[dcindex] = unstable_co;
				}
				
				
				for(int taskindex = 0; taskindex < numberOfTask; taskindex++) {
					Task task = tasklist.get(taskindex);
					int datanumber = (int)data[taskindex];
					for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
						int xindex = taskindex*Parameters.numberOfDC + dcindex;
						double mi_mu = Parameters.MIPSbaselineOfDC[dcindex];
	        			double mi_sigmaco = Parameters.bwHeterogeneityCVOfDC[dcindex];
	        			double io_mu = Parameters.ioBaselineOfDC[dcindex];
	        			double io_sigmaco = Parameters.ioHeterogeneityCVOfDC[dcindex];
	        			double bw_mu = Parameters.bwBaselineOfDC[dcindex];
	        			double bw_mu_ori = bw_mu;
	        			double bw_sigmaco = Parameters.bwHeterogeneityCVOfDC[dcindex];
	        			
	        			double io_mu_ori = io_mu;
	        			double mi_mu_ori = mi_mu;
	        			
	        			double miSeconds = task.getMi()/mi_mu;
	        			double ioSeconds = task.getIo()/io_mu;
	        			double bwlength = TotalTransferDataSize[xindex]/1024d;
	        			double bwSeconds = TotalTransferDataSize[xindex]/(bw_mu*1024d);
	        			
	        			if (task.getMi() > 0 && miSeconds >= Math.max(ioSeconds, bwSeconds)) {
	        				
	        				io_mu = Math.min(io_mu_ori, task.getIo()*mi_mu/task.getMi());
	        				bw_mu = Math.min(bw_mu_ori, bwlength*mi_mu/task.getMi());
	        			} else if (task.getIo() > 0 && ioSeconds >= Math.max(miSeconds, bwSeconds)) {
	        				mi_mu = Math.min(mi_mu_ori, task.getMi()*io_mu/task.getIo());
	        				bw_mu = Math.min(bw_mu_ori, bwlength*io_mu/task.getIo());
	        				
	        			} else if (TotalTransferDataSize[xindex] > 0 && bwSeconds >= Math.max(miSeconds, ioSeconds)) {
	        				mi_mu = Math.min(mi_mu_ori, task.getMi()*bw_mu/bwlength);
	        				io_mu = Math.min(io_mu_ori, task.getIo()*bw_mu/bwlength);
	        				
	        			}
	        			
	        			// bandwidth bandwidth_dataDelay_co bandwidth_dataDelayOfTaskInDC
	        			double bandwidthco = bw_mu/bw_mu_ori;
	        			bandwidth_dataDelayOfTaskInDC[0][xindex] = 0;
						for(int dataindex = 0; dataindex < datanumber; dataindex++) {
							
							if (TotalTransferDataSize[xindex] > 0) {
								bandwidth[xindex][dataindex] *= bandwidthco;
								task.bandwidth[dcindex][dataindex] *= bandwidthco;
								// wait for compute
								int dataindex_pos = (int) datapos[taskindex][dataindex];
								double dataDelay = Parameters.delayAmongDCIndex[dataindex_pos][dcindex];
								if(transferDataSize[xindex][dataindex]==0) {
									bandwidth_dataDelay_co[xindex][dataindex] = 0;
								}else {
									bandwidth_dataDelay_co[xindex][dataindex] = transferDataSize[xindex][dataindex]
											/(transferDataSize[xindex][dataindex]+dataDelay*bandwidth[xindex][dataindex]*1024d);

								}
								
							}else {
								bandwidth[xindex][dataindex] = 0;
								bandwidth_dataDelay_co[xindex][dataindex] = 0;
							}
							// change the position
							bandwidth_dataDelayOfTaskInDC[0][xindex] += 
									bandwidth[xindex][dataindex] * bandwidth_dataDelay_co[xindex][dataindex];
						}
						
						double mi_sigma = mi_mu * mi_sigmaco;
						double io_sigma = io_mu * io_sigmaco;
						double bw_mu_dataDelay = bandwidth_dataDelayOfTaskInDC[0][xindex];
						
						muParaOfTaskInDC[xindex] = (mi_mu + io_mu + bw_mu_dataDelay) * unstablecoOfDC[dcindex];
	        			sigmaParaOfTaskInDC[xindex] = unstablecoOfDC[dcindex] * Math.sqrt(Math.pow(mi_sigma, 2)
	        					+Math.pow(io_sigma, 2)+Math.pow((bw_mu_dataDelay*bw_sigmaco), 2));
	        			double delay_para = (double)Parameters.delayAmongDCIndex[task.submitDCIndex][dcindex];
	        			// allRateMuArray allRateSigmaArray
	        			double task_workload = task.getMi() + task.getIo() + bwlength;
	        			double delay_co = task_workload/(task_workload + muParaOfTaskInDC[xindex] * delay_para);
	        			allRateMuArray[0][xindex] = muParaOfTaskInDC[xindex] * delay_co;
	        			allRateSigmaArray[0][xindex] = sigmaParaOfTaskInDC[xindex] * delay_co;
	        			objParaOfTaskInDC.get(task.getCloudletId()).put(dcindex, 
	        					allRateMuArray[0][xindex]
	        					+ Parameters.r * allRateSigmaArray[0][xindex]); 
	        			
					}
					tasklist.set(taskindex, task);
				}
				
				
				
				
				
				
				
				//SlotArray UpArray DownArray
				
				for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
					if(scheduler.healthyStateOfDC.get(dcindex + DCbase) == true) {
						SlotArray[0][dcindex] = scheduler.getIdleTaskSlotsOfDC().get(dcindex + DCbase).size();
					}else {
						SlotArray[0][dcindex] = 0;
					}
					UpArray[0][dcindex] = scheduler.getUplinkOfDC().get(dcindex + DCbase);
					DownArray[0][dcindex] = scheduler.getDownlinkOfDC().get(dcindex + DCbase);
				}
				
				
				// store the data into the job
				
				job.muParaOfTaskInDC = muParaOfTaskInDC;
				job.sigmaParaOfTaskInDC = sigmaParaOfTaskInDC;
				
				job.probArray = probArray;
				job.data = data;
				job.datapos = datapos;
				job.bandwidth = bandwidth;
				job.bandwidth_dataDelayOfTaskInDC = bandwidth_dataDelayOfTaskInDC;
				job.bandwidth_dataDelay_co = bandwidth_dataDelay_co;

				job.allRateMuArray = allRateMuArray;
				job.allRateSigmaArray = allRateSigmaArray;
				
				job.objParaOfTaskInDC = objParaOfTaskInDC;
			    
				job.TotalTransferDataSize = TotalTransferDataSize;
				job.transferDataSize = transferDataSize;
				
				job.currentGreatePosition.clear();
				job.currentGreateRate.clear();
				// 
				job.failedAssignTaskIndexInGreateAssign.clear();
				
				if(job.sortedflag == false) {
					job.sortedListOfTask = new HashMap<>();
					for(int tindex = 0; tindex < numberOfTask; tindex++) {
						Task task = tasklist.get(tindex);
						int taskId = task.getCloudletId();
						Map<Integer, Double> map = objParaOfTaskInDC.get(taskId);
						job.sortedListOfTask.put(taskId, new ArrayList<>());
						for(Map.Entry<Integer, Double> entry:map.entrySet()) {
							job.sortedListOfTask.get(taskId).add(entry);
						}
						job.sortedListOfTask.get(taskId).sort(new Comparator<Map.Entry<Integer, Double>>() {

							@Override
							public int compare(Entry<Integer, Double> o1, Entry<Integer, Double> o2) {
								// TODO Auto-generated method stub
								return o2.getValue().compareTo(o1.getValue());
							}
							
						});
						int listindex = 0;
						for(Map.Entry<Integer, Double> iterm:job.sortedListOfTask.get(taskId)) {
							int dcindex = iterm.getKey();
							task.orderedDClist[listindex] = dcindex;
							listindex++;
						}
						tasklist.set(tindex, task);
						job.unscheduledGreateRate.put(taskId, job.sortedListOfTask.get(taskId).get(0).getValue());
						
						job.unscheduledGreatePosition.put(taskId, new ArrayList<>());
						job.unscheduledGreatePosition.get(taskId).add(job.sortedListOfTask.get(taskId).get(0).getKey());
						for(int posindex = 1; posindex < Parameters.numberOfDC; posindex++) {
							if(job.unscheduledGreateRate.get(taskId) > job.sortedListOfTask.get(taskId).get(posindex).getValue()) {
								break;
							}else {
								job.unscheduledGreatePosition.get(taskId).add(job.sortedListOfTask.get(taskId).get(posindex).getKey());
							}
						}
					}
					job.sortedflag = true;
				}
				
				
				// uselessDCforTask
				// wait for verify
				
				for(int taskindex = 0; taskindex < numberOfTask; taskindex++) {
					Task task = tasklist.get(taskindex);
					int taskId = task.getCloudletId();
					double maxRate = job.unscheduledGreateRate.get(taskId);
					for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
						int xindex = taskindex * Parameters.numberOfDC + dcindex;
						if(objParaOfTaskInDC.get(taskId).get(dcindex) <= (maxRate * 1/(1+Parameters.epsilon))) {
							uselessDCforTask[xindex] = 0;
							task.uselessDC[dcindex] = 0;
							uselessConstraintsNum += 1;
						}
					}
					boolean noCandidateDCflag = true;
					for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
						int xindex = taskindex * Parameters.numberOfDC + dcindex;
						if(uselessDCforTask[xindex]==-1) {
							noCandidateDCflag = false;
							break;
						}	
					}
					if(noCandidateDCflag == true) {
						for(int bdcindex = 0; bdcindex < job.unscheduledGreatePosition.get(taskId).size(); bdcindex++) {
							int bxindex = taskindex * Parameters.numberOfDC + job.unscheduledGreatePosition.get(taskId).get(bdcindex);
							uselessDCforTask[bxindex] = -1;
							task.uselessDC[job.unscheduledGreatePosition.get(taskId).get(bdcindex)] = -1;
							uselessConstraintsNum -= 1;
						}
						
					}
					tasklist.set(taskindex, task);
				}
				
//				for(int taskindex = 0; taskindex < numberOfTask; taskindex++) {
//					Task task = tasklist.get(taskindex);
//					int taskId = task.getCloudletId();
//					int datanumber = data[taskindex];
//					// best vm in best DC
//					int randomeindex = (int)Math.round(Math.random()*(job.unscheduledGreatePosition.get(task.getCloudletId()).size()-1));
//					int bestDCindex = job.unscheduledGreatePosition.get(task.getCloudletId()).get(randomeindex);
//					int bestxindex = taskindex * Parameters.numberOfDC + bestDCindex;
//					List<Vm> vmList = scheduler.getIdleTaskSlotsOfDC().get(bestDCindex+DCbase);
//					int vmSize = vmList.size();
//					double maxRate = Double.MIN_VALUE;
//					for(int j=0; j<vmSize; j++) {
//						DynamicVm vm = (DynamicVm)vmList.get(j);
//						
//						//compute the rate
//						double mips = vm.getMips();
//						double bwps = vm.getBw();
//						double iops = vm.getIo();
//						double mips_ori = mips;
//						double bwps_ori = bwps;
//						double iops_ori = iops;
//						double miSeconds = task.getMi()/mips;
//	        			double ioSeconds = task.getIo()/iops;
//	        			double bwSeconds = TotalTransferDataSize[bestxindex]/bwps;
//	        			
//	        			if (task.getMi() > 0 && miSeconds >= Math.max(ioSeconds, bwSeconds)) {
//	        				iops = Math.min(iops_ori, task.getIo()*mips/task.getMi());
//	        				bwps = Math.min(bwps_ori,TotalTransferDataSize[bestxindex]*mips/task.getMi());
//	        			} else if (task.getIo() > 0 && ioSeconds >= Math.max(miSeconds, bwSeconds)) {
//	        				mips = Math.min(mips_ori,task.getMi()*iops/task.getIo());
//	        				bwps = Math.min(bwps_ori,TotalTransferDataSize[bestxindex]*iops/task.getIo());
//	        				
//	        			} else if (TotalTransferDataSize[bestxindex] > 0 && bwSeconds >= Math.max(miSeconds, ioSeconds)) {
//	        				mips = Math.min(mips_ori,task.getMi()*bwps/TotalTransferDataSize[bestxindex]);
//	        				iops = Math.min(iops_ori,task.getIo()*bwps/TotalTransferDataSize[bestxindex]);
//	        				
//	        			}
//	        			double[] bandwidthInBestVM = new double[Parameters.ubOfData];
//	        			double[] bandwidth_dataDelay_coInBestVM = new double[Parameters.ubOfData];
//	        			double bandwidth_dataDelayInBestVM = 0d;
//	        			for(int dataindex = 0; dataindex < datanumber; dataindex++) {
//	        				if (TotalTransferDataSize[bestxindex] > 0) {
//								bandwidthInBestVM[dataindex] = bwps*transferDataSize[bestxindex][dataindex]/TotalTransferDataSize[bestxindex];
//								
//								int dataindex_pos = (int) datapos[taskindex][dataindex];
//								double dataDelay = Parameters.delayAmongDCIndex[dataindex_pos][bestDCindex];
//								if(transferDataSize[bestxindex][dataindex]==0) {
//									bandwidth_dataDelay_coInBestVM[dataindex] = 0;
//								}else {
//									bandwidth_dataDelay_coInBestVM[dataindex] = transferDataSize[bestxindex][dataindex]
//											/(transferDataSize[bestxindex][dataindex]+dataDelay*bandwidthInBestVM[dataindex]);
//
//								}
//	        				
//	        				}else {
//								bandwidthInBestVM[dataindex] = 0;
//								bandwidth_dataDelay_coInBestVM[dataindex] = 0;
//							}
//	        				bandwidth_dataDelayInBestVM += 
//									bandwidthInBestVM[dataindex] * bandwidth_dataDelay_coInBestVM[dataindex];
//						
//	        				
//	        			}
//	        			
//						
//						double orirate = (mips + iops + bandwidth_dataDelayInBestVM)*unstablecoOfDC[bestDCindex];
//						double delay_para = (double)Parameters.delayAmongDCIndex[task.submitDCIndex][bestDCindex];
//	        			
//	        			double task_workload = task.getMi() + task.getIo() + TotalTransferDataSize[bestxindex];
//	        			double delay_co = task_workload/(task_workload + orirate * delay_para);
//	        			double rate = orirate * delay_co;
//	        			
//						if(rate > maxRate) {
//							maxRate = rate;
//						}
//					}
//					for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
//						int xindex = taskindex * Parameters.numberOfDC + dcindex;
//						if(objParaOfTaskInDC.get(task.getCloudletId()).get(dcindex) <= (maxRate * 1/(1+Parameters.epsilon))) {
//							uselessDCforTask[xindex] = 0;
//							uselessConstraintsNum += 1;
//						}
//					}
//					boolean noCandidateDCflag = true;
//					for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
//						int xindex = taskindex * Parameters.numberOfDC + dcindex;
//						if(uselessDCforTask[xindex]==-1) {
//							noCandidateDCflag = false;
//						}	
//					}
//					if(noCandidateDCflag == true) {
//						for(int bdcindex = 0; bdcindex < job.unscheduledGreatePosition.get(taskId).size(); bdcindex++) {
//							int bxindex = taskindex * Parameters.numberOfDC + job.unscheduledGreatePosition.get(taskId).get(bdcindex);
//							uselessDCforTask[bxindex] = -1;
//							uselessConstraintsNum -= 1;
//						}
//						
//					}
//				}
				
				job.uselessConstraintsNum = uselessConstraintsNum;
				job.uselessDCforTask = uselessDCforTask;
				
				
				
				
				int constraintsNum = 2 * numberOfTask + 3 * Parameters.numberOfDC + uselessConstraintsNum;
				IloRange[] rng = new IloRange[constraintsNum];
				int constraintIndex = 0;
				// constraints
				// extra constraints about task
				for(int tindex = 0; tindex < numberOfTask; tindex++) {
					IloNumExpr[] itermOfTask = new IloNumExpr[vnum + 1];
					for(int taskindex = 0; taskindex < numberOfTask; taskindex++) {
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							int xindex = taskindex*Parameters.numberOfDC + dcindex;
							if(taskindex == tindex) {
								itermOfTask[xindex] = cplex.prod((allRateMuArray[0][xindex]
										+ Parameters.r * allRateSigmaArray[0][xindex]), var[xindex]);
							}else {
								itermOfTask[xindex] = cplex.prod(0.0, var[xindex]);
							}
						}
					}
					itermOfTask[vnum] = cplex.prod(-1.0, var[vnum]);
					rng[constraintIndex] = cplex.addGe(cplex.sum(itermOfTask), 0.0);
					constraintIndex++;
				}
				
				// each task has one execution among DCs
				for(int tindex = 0; tindex < numberOfTask; tindex++) {
					IloNumExpr[] itermOfTask = new IloNumExpr[vnum + 1];
					for(int taskindex = 0; taskindex < numberOfTask; taskindex++) {
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							int xindex = taskindex*Parameters.numberOfDC + dcindex;
							if(taskindex == tindex) {
								itermOfTask[xindex] = cplex.prod(1.0, var[xindex]);
							}else {
								itermOfTask[xindex] = cplex.prod(0.0, var[xindex]);
							}
						}
					}
					itermOfTask[vnum] = cplex.prod(0.0, var[vnum]);
					rng[constraintIndex] = cplex.addEq(cplex.sum(itermOfTask), 1.0);
					constraintIndex++;
				}
				
				// machine limitation
				for(int datacenterindex = 0; datacenterindex < Parameters.numberOfDC; datacenterindex++) {
					IloNumExpr[] itermOfTask = new IloNumExpr[vnum + 1];
					for(int taskindex = 0; taskindex < numberOfTask; taskindex++) {
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							int xindex = taskindex*Parameters.numberOfDC + dcindex;
							if(dcindex == datacenterindex) {
								itermOfTask[xindex] = cplex.prod(1.0, var[xindex]);
							}else {
								itermOfTask[xindex] = cplex.prod(0.0, var[xindex]);
							}
						}
					}
					itermOfTask[vnum] = cplex.prod(0.0, var[vnum]);
					rng[constraintIndex] = cplex.addLe(cplex.sum(itermOfTask), SlotArray[0][datacenterindex]);
					constraintIndex++;
				}
				
				// uplink bandwidth limitation
				for(int datacenterindex = 0; datacenterindex < Parameters.numberOfDC; datacenterindex++) {
					IloNumExpr[] itermOfTask = new IloNumExpr[vnum + 1];
					for(int taskindex = 0; taskindex < numberOfTask; taskindex++) {
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							int xindex = taskindex*Parameters.numberOfDC + dcindex;
							double upsum = 0d;
							for(int dataindex = 0; dataindex < data[taskindex]; dataindex++) {
								if(datapos[taskindex][dataindex] == datacenterindex) {
									upsum += bandwidth[xindex][dataindex];
								}
							}
							itermOfTask[xindex] = cplex.prod(upsum, var[xindex]);
						}
					}
					itermOfTask[vnum] = cplex.prod(0.0, var[vnum]);
					rng[constraintIndex] = cplex.addLe(cplex.sum(itermOfTask), UpArray[0][datacenterindex]);
					constraintIndex++;
				}
				
				// downlink bandwidth limitation
				
				for(int datacenterindex = 0; datacenterindex < Parameters.numberOfDC; datacenterindex++) {
					IloNumExpr[] itermOfTask = new IloNumExpr[vnum + 1];
					for(int taskindex = 0; taskindex < numberOfTask; taskindex++) {
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							int xindex = taskindex*Parameters.numberOfDC + dcindex;
							if(dcindex == datacenterindex) {
								double downsum = 0d;
								for(int dataindex = 0; dataindex < data[taskindex]; dataindex++) {
										downsum += bandwidth[xindex][dataindex];
								}
								itermOfTask[xindex] = cplex.prod(downsum, var[xindex]);
							}else {
								itermOfTask[xindex] = cplex.prod(0.0, var[xindex]);
							}
						}
					}
					itermOfTask[vnum] = cplex.prod(0.0, var[vnum]);
					rng[constraintIndex] = cplex.addLe(cplex.sum(itermOfTask), DownArray[0][datacenterindex]);
					constraintIndex++;
				}
				// uselessDC limitation
				for(int xindex = 0; xindex < vnum; xindex++) {
					if(uselessDCforTask[xindex] == 0) {
						rng[constraintIndex] = cplex.addEq(cplex.prod(1.0, var[xindex]),0.0);
						constraintIndex++;
					}
				}
				
				if(cplex.solve()) {
					double[] x = new double[vnum];
					double[] vresult = cplex.getValues(var);
					for(int vindex = 0; vindex < vnum; vindex++) {
						x[vindex] = vresult[vindex];
					}
					
					double[] slack = cplex.getSlacks(rng);
					System.out.println("Solution status = " + cplex.getStatus());
					
					//verify x
					double[] tempSlotArray = new double[Parameters.numberOfDC];
					double[] tempUpArray = new double[Parameters.numberOfDC];
					double[] tempDownArray = new double[Parameters.numberOfDC];
					
					for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
						tempSlotArray[dcindex] = SlotArray[0][dcindex];
						tempUpArray[dcindex] = UpArray[0][dcindex];
						tempDownArray[dcindex] = DownArray[0][dcindex];
					}
					
					for(int tindex = 0; tindex < numberOfTask; tindex++) {
						boolean success = false;
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							int datanumber = data[tindex];
							int xindex = tindex * Parameters.numberOfDC + dcindex;
							if(x[xindex] > 0 && success == false) {
								
								boolean resourceEnough = true;
								// machines
								if((tempSlotArray[dcindex]-1)<0) {
									resourceEnough = false;
								}
								
								
								double totalBandwidth = 0d;
								// uplink
								Map<Integer, Double> bwOfSrcPos = new HashMap<>();
								if(TotalTransferDataSize[xindex]>0 && resourceEnough == true) {
									for(int dataindex = 0; dataindex < datanumber; dataindex++) {
										double neededBw = bandwidth[xindex][dataindex];
										totalBandwidth+=neededBw;
										int srcPos = (int) datapos[tindex][dataindex];
										if(bwOfSrcPos.containsKey(srcPos)) {
											double oldvalue = bwOfSrcPos.get(srcPos);
											bwOfSrcPos.put(srcPos, oldvalue + neededBw);
										}else {
											bwOfSrcPos.put(srcPos, 0 + neededBw);
										}
									}
									for(int pos : bwOfSrcPos.keySet()) {
										if((tempUpArray[pos]-bwOfSrcPos.get(pos))<0) {
											resourceEnough = false;
											break;
										}
									}
								}
								
								// downlink
								if(TotalTransferDataSize[xindex]>0 && resourceEnough == true) {
									if((tempDownArray[dcindex]-totalBandwidth)<0) {
										resourceEnough = false;
									}
								}
								
								if(resourceEnough == true) {
									tempSlotArray[dcindex] -= 1;
									if(TotalTransferDataSize[xindex]>0) {
										tempDownArray[dcindex] -= totalBandwidth;

									}
									for(int pos : bwOfSrcPos.keySet()) {
										tempUpArray[pos] -= bwOfSrcPos.get(pos);
									}
									success = true;
									x[xindex] = 1;
								}else {
									x[xindex] = 0;
								}
							}else {
								x[xindex] = 0;
							}
						}
					}
					
					
					// 
					
					
					// store the greatest assignment info in the job with the current resource
					for(int tindex = 0; tindex < numberOfTask; tindex++) {
						Task task = tasklist.get(tindex);
						double rate = 0.0d;
						int pos = 0;
						boolean greatAssignSuccess = false;
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							int xindex = tindex*Parameters.numberOfDC+dcindex;
							if(x[xindex]==1) {
								rate = allRateMuArray[0][xindex] + Parameters.r * allRateSigmaArray[0][xindex];
								pos = dcindex;
								greatAssignSuccess = true;
								break;
							}
						}
						if(greatAssignSuccess == true) {
							job.currentGreateRate.put(task.getCloudletId(), rate);
							job.currentGreatePosition.put(task.getCloudletId(), pos);
						}else {
							job.failedAssignTaskIndexInGreateAssign.add(tindex);
						}
						
					}
					
					job.greatX = x;
					cplex.end();
					
				}else {
					// greedy assign for the tasks in the job as well as its copy
					// when there is some tasks do not be assigned then the copy is not needed
					// use the matlab jar
					
					double[] x = new double[vnum];
					double[] tempSlotArray = new double[Parameters.numberOfDC];
					double[] tempUpArray = new double[Parameters.numberOfDC];
					double[] tempDownArray = new double[Parameters.numberOfDC];
					for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
						tempSlotArray[dcindex] = SlotArray[0][dcindex];
						tempUpArray[dcindex] = UpArray[0][dcindex];
						tempDownArray[dcindex] = DownArray[0][dcindex];
					}
					
					for(int tindex = 0; tindex < numberOfTask; tindex++) {
						Task task = tasklist.get(tindex);
						int taskId = task.getCloudletId();
						
						
						boolean success = true;
						int successDC = -1;
						for(Map.Entry<Integer, Double> iterm:job.sortedListOfTask.get(taskId)) {
							int dcindex = iterm.getKey();
							int datanumber = data[tindex];
							int xindex = tindex * Parameters.numberOfDC + dcindex;
							success = true;
							
							if(uselessDCforTask[xindex] == 0) {
								success = false;
								break;
							}
							
							// when the dc is not too far
							if(uselessDCforTask[xindex] != 0) {
								// verify that the resource is enough
									
									// machines
								if((tempSlotArray[dcindex]-1)<0) {
									success = false;
									continue;
								}
								
								
								double totalBandwidth = 0d;
								// uplink
								Map<Integer, Double> bwOfSrcPos = new HashMap<>();
								if(TotalTransferDataSize[xindex]>0) {
									for(int dataindex = 0; dataindex < datanumber; dataindex++) {
										double neededBw = bandwidth[xindex][dataindex];
										totalBandwidth += neededBw;
										int srcPos = (int) datapos[tindex][dataindex];
										if(bwOfSrcPos.containsKey(srcPos)) {
											double oldvalue = bwOfSrcPos.get(srcPos);
											bwOfSrcPos.put(srcPos, oldvalue + neededBw);
										}else {
											bwOfSrcPos.put(srcPos, 0 + neededBw);
										}
									}
									for(int pos : bwOfSrcPos.keySet()) {
										if((tempUpArray[pos]-bwOfSrcPos.get(pos))<0) {
											success = false;
											break;
										}
									}
								}
								
								// downlink
								if(TotalTransferDataSize[xindex]>0 && success == true) {
									if((tempDownArray[dcindex]-totalBandwidth)<0) {
										success = false;
										continue;
									}
								}
								if(success == true) {
									tempSlotArray[dcindex] -= 1;
									if(TotalTransferDataSize[xindex]>0) {
										tempDownArray[dcindex] -= totalBandwidth;
									}
									
									for(int pos : bwOfSrcPos.keySet()) {
										tempUpArray[pos] -= bwOfSrcPos.get(pos);
									}
									successDC = dcindex;
									break;
								}
							}
						}
						if(success == true && successDC != -1) {
							// store the greatest assignment info in the job with the current resource
							int xindex = tindex * Parameters.numberOfDC + successDC;
							job.currentGreateRate.put(taskId, allRateMuArray[0][xindex]
									+ Parameters.r * allRateSigmaArray[0][xindex]);
							job.currentGreatePosition.put(taskId, successDC);
							x[xindex] = 1;
						}else {
							job.failedAssignTaskIndexInGreateAssign.add(tindex);
						}
					}
					
					job.greatX = x;
					
				}
				
				
			} catch (IloException e) {
				System.err.println("Concert exception caught '" + e + "' caught");
				e.printStackTrace();
			}catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
        	
        	
        }
        
        
        
        for (int i = 0; i < size; i++) {
            int maxIndex = 0;
            Job maxJob = null;
            for (int j = 0; j < size; j++) {
                Job job = (Job) getCloudletList().get(j);
                if (!hasChecked.get(j)) {
                    maxJob = job;
                    maxIndex = j;
                    break;
                }
            }
            if (maxJob == null) {
                break;
            }


            for (int j = 0; j < size; j++) {
            	Job job = (Job) getCloudletList().get(j);
                if (hasChecked.get(j)) {
                    continue;
                }
                double utility = job.getJobUtility();
                if (utility > maxJob.getJobUtility()) {
                    maxJob = job;
                    maxIndex = j;
                }
            }
            hasChecked.set(maxIndex, true);

            
            //getScheduledList().add(minCloudlet);
            getRankedList().add(maxJob);
        }
    }

}
