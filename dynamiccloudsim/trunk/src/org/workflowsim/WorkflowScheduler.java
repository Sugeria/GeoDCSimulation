/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.scheduling.DataAwareSchedulingAlgorithm;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.scheduling.FCFSSchedulingAlgorithm;
import org.workflowsim.scheduling.MCTSchedulingAlgorithm;
import org.workflowsim.scheduling.MaxMinSchedulingAlgorithm;
import org.workflowsim.scheduling.MinMinSchedulingAlgorithm;
import org.workflowsim.scheduling.MinRateSchedulingAlgorithm;
import org.workflowsim.scheduling.MinSchedulingAlgorithm;
import org.workflowsim.scheduling.RoundRobinSchedulingAlgorithm;
import org.workflowsim.scheduling.StaticSchedulingAlgorithm;

import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWComplexity;
import com.mathworks.toolbox.javabuilder.MWException;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import com.sun.xml.internal.ws.api.streaming.XMLStreamReaderFactory.Default;

import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.examples.Parameters.SchedulingAlgorithm;
import de.huberlin.wbi.dcs.workflow.Task;
import ilog.concert.IloException;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;
import ilog.concert.IloRange;
import ilog.cplex.IloCplex;
//import matlabcontrol.MatlabInvocationException;
//import matlabcontrol.MatlabProxy;
//import matlabcontrol.MatlabProxyFactory;
//import matlabcontrol.extensions.MatlabNumericArray;
//import matlabcontrol.extensions.MatlabTypeConverter;
import taskAssign.TaskAssign;


/**
 * WorkflowScheduler represents a algorithm acting on behalf of a user. It hides
 * VM management, as vm creation, sumbission of jobs to this VMs and destruction
 * of VMs. It picks up a scheduling algorithm based on the configuration
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class WorkflowScheduler extends DatacenterBroker {

    /**
     * The workflow engine id associated with this workflow algorithm.
     */
    private int workflowEngineId;
    private Map<Integer, Vm> vms;
    protected LinkedList<Task> taskQueue; 
    private int taskSlotsPerVm;
    // two collections of tasks, which are currently running;
 	// note that the second collections is a subset of the first collection
 	private Map<Integer, Task> tasks;
 	private Map<Integer, Queue<Task>> speculativeTasks;
	private Map<Integer, LinkedList<Vm>> idleTaskSlotsOfDC;
	private Map<Integer, Integer> taskOfJob;
	private Map<Integer, Integer> ackTaskOfJob;
	private Map<Integer, Integer> scheduledTaskOfJob;
	private Map<Integer, Job> JobFactory;
	private double runtime;
	
//	private MatlabProxyFactory factory;
//	public MatlabProxy proxy;

	
    /**
     * Created a new WorkflowScheduler object.
     *
     * @param name name to be associated with this entity (as required by
     * Sim_entity class from simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public WorkflowScheduler(String name,int taskSlotsPerVm) throws Exception {
        super(name);
        taskQueue = new LinkedList<>();
		tasks = new HashMap<>();
		speculativeTasks = new HashMap<>();
		vms = new HashMap<>();
		this.taskSlotsPerVm = taskSlotsPerVm;
		idleTaskSlotsOfDC = new HashMap<>();
		taskOfJob = new HashMap<>();
		ackTaskOfJob = new HashMap<>();
		scheduledTaskOfJob = new HashMap<>();
		JobFactory = new HashMap<>();
//		factory = new MatlabProxyFactory();
//		proxy = factory.getProxy();
		
    }
    
    public void taskReady(Task task) {
		taskQueue.add(task);

	}

	public boolean tasksRemaining() {
//		return !taskQueue.isEmpty()
//				|| (tasks.size() > speculativeTasks.size() && speculativeTasks
//						.size() < speculativeCapAbs);
		return !taskQueue.isEmpty();
	}

	public void taskSucceeded(Task task, Vm vm) {
		tasks.remove(task);
		speculativeTasks.remove(task);
	}
    
	public LinkedList<Task> getTaskQueue() {
		// TODO Auto-generated method stub
		return taskQueue;
	}


    /**
     * Binds this scheduler to a datacenter
     *
     * @param datacenterId data center id
     */
    public void bindSchedulerDatacenter(int datacenterId) {
        if (datacenterId <= 0) {
            Log.printLine("Error in data center id");
            return;
        }
        this.datacenterIdsList.add(datacenterId);
    }

    /**
     * Sets the workflow engine id
     *
     * @param workflowEngineId the workflow engine id
     */
    public void setWorkflowEngineId(int workflowEngineId) {
        this.workflowEngineId = workflowEngineId;
    }

    @Override
	protected void updateTaskUsedBandwidth(SimEvent ev) {
		super.updateTaskUsedBandwidth(ev);
		Task task = (Task)ev.getData();
		if(task.isSpeculativeCopy()) {
			LinkedList<Task> speculativeTaskOfTask = (LinkedList<Task>)speculativeTasks
					.get(task.getCloudletId());
			for (int index = 0; index < speculativeTaskOfTask.size(); index++ ) {
				if (task.getVmId() == speculativeTaskOfTask.get(index).getVmId()) {
					speculativeTaskOfTask.set(index, task);
					break;
				}
			}
			speculativeTasks.put(task.getCloudletId(), speculativeTaskOfTask);
		} else {
			tasks.put(task.getCloudletId(), task);
		}
	}
    
    
    /**
     * Switch between multiple schedulers. Based on algorithm.method
     *
     * @param name the SchedulingAlgorithm name
     * @return the algorithm that extends BaseSchedulingAlgorithm
     */
    private BaseSchedulingAlgorithm getScheduler(SchedulingAlgorithm name) {
        BaseSchedulingAlgorithm algorithm;

        // choose which algorithm to use. Make sure you have add related enum in
        //Parameters.java
        switch (name) {
            //by default it is Static
            case FCFS:
                algorithm = new FCFSSchedulingAlgorithm();
                break;
            case MINMIN:
                algorithm = new MinMinSchedulingAlgorithm();
                break;
            case MAXMIN:
                algorithm = new MaxMinSchedulingAlgorithm();
                break;
            case MCT:
                algorithm = new MCTSchedulingAlgorithm();
                break;
            case DATA:
                algorithm = new DataAwareSchedulingAlgorithm();
                break;
            case STATIC:
                algorithm = new StaticSchedulingAlgorithm();
                break;
            case ROUNDROBIN:
                algorithm = new RoundRobinSchedulingAlgorithm();
                break;
            case MIN:
            	algorithm = new MinSchedulingAlgorithm();
            	break;
            case MINRATE:
            	algorithm = new MinRateSchedulingAlgorithm();
            	break;
            default:
                algorithm = new StaticSchedulingAlgorithm();
                break;

        }
        return algorithm;
    }

    

    /**
     * Update a cloudlet (job)
     *
     * @param ev a simEvent object
     */
    @Override
    protected void processCloudletUpdate(SimEvent ev) {

    	TaskAssign taskAssign = null;
        BaseSchedulingAlgorithm scheduler = getScheduler(Parameters.getSchedulingAlgorithm());
        scheduler.DCbase = DCbase;
        scheduler.workflowScheduler = this;
        scheduler.setCloudletList(getCloudletList());
        // for MinRateScheduler the command is not needed
        //scheduler.setVmList(getVmsCreatedList());

        
        try {
            scheduler.run();
        } catch (Exception e) {
            Log.printLine("Error in configuring scheduler_method");
            e.printStackTrace();
        }

        List<Cloudlet> rankedList = scheduler.getRankedList();
        double[][] SlotArray = new double[1][Parameters.numberOfDC];
		double[][] UpArray = new double[1][Parameters.numberOfDC];
		double[][] DownArray = new double[1][Parameters.numberOfDC];
		int slotNum = 0;
        //SlotArray UpArray DownArray
		
  		for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
  			if(healthyStateOfDC.get(dcindex + DCbase) == true) {
  				SlotArray[0][dcindex] = idleTaskSlotsOfDC.get(dcindex + DCbase).size();
  				slotNum += SlotArray[0][dcindex];
  			}else {
  				SlotArray[0][dcindex] = 0;
  			}
  			UpArray[0][dcindex] = getUplinkOfDC().get(dcindex + DCbase);
  			DownArray[0][dcindex] = getDownlinkOfDC().get(dcindex + DCbase);
  		}
  		
  		int remainingSlotNum = slotNum;
        
        int jobNumInOneLoop = rankedList.size();
        // each job weight equal 1
        
        for (int jobindex = 0; jobindex < jobNumInOneLoop; jobindex++) {
        	Job job = (Job)rankedList.get(jobindex);
        	// submitTasks while update JobList info
			if(!taskOfJob.containsKey(job.getCloudletId())) {
				if(job.getTaskList().size() == 0) {
					job.jobId = job.getCloudletId();
    				taskOfJob.put(job.getCloudletId(), 1);
    				ackTaskOfJob.put(job.getCloudletId(), 0);
    				JobFactory.put(job.getCloudletId(), job);
    			}else {
    				// when return remember to delete the item in the three tables
    				taskOfJob.put(job.getCloudletId(), job.getTaskList().size());
    				scheduledTaskOfJob.put(job.getCloudletId(), 0);
    				ackTaskOfJob.put(job.getCloudletId(), 0);
    				JobFactory.put(job.getCloudletId(), job);
    			}
			}
        	int preAssignedSlots = Math.min(remainingSlotNum,(int)Math.round(slotNum/(Parameters.epsilon*jobNumInOneLoop)));
        	int unscheduledTaskNum = job.unscheduledTaskList.size();
        	int greatAssignedTaskNum = unscheduledTaskNum - job.failedAssignTaskIndexInGreateAssign.size();
        	if(greatAssignedTaskNum == 0)
        		continue;
        	// whether preAssigned slots is enough for the original tasks in the job
        	double[] singlex = null;
        	Map<Integer, Double> bwOfSrcPos = null;
        	boolean allzeroflag = true;
        	int vnum = unscheduledTaskNum*Parameters.numberOfDC;
        	if(greatAssignedTaskNum <= preAssignedSlots) {
        	// if yes
        		// job great assignment whether violate allocated resource
        		// entire resource for a job
        		double[] tempSlotArray = new double[Parameters.numberOfDC];
				double[] tempUpArray = new double[Parameters.numberOfDC];
				double[] tempDownArray = new double[Parameters.numberOfDC];
				for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
					tempSlotArray[dcindex] = SlotArray[0][dcindex];
					tempUpArray[dcindex] = UpArray[0][dcindex];
					tempDownArray[dcindex] = DownArray[0][dcindex];
				}
            	boolean resourceEnough = true;
            	
            	for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
            		int datanumber = job.data[tindex];
            		int taskId = job.unscheduledTaskList.get(tindex).getCloudletId();
            		if(job.currentGreatePosition.containsKey(taskId)) {
            			int pos = job.currentGreatePosition.get(taskId);
            			int xindex = tindex * Parameters.numberOfDC + pos;
            			if((tempSlotArray[pos]-1)<0) {
							resourceEnough = false;
						}else {
							tempSlotArray[pos]-=1;
						}
						
						// downlink
						if(job.TotalTransferDataSize[xindex]>0) {
							if((tempDownArray[pos]-Parameters.bwBaselineOfDC[pos])<0) {
								resourceEnough = false;
							}else {
								tempDownArray[pos]-=Parameters.bwBaselineOfDC[pos];
							}
						}
						
						// uplink
						bwOfSrcPos = new HashMap<>();
						if(job.TotalTransferDataSize[xindex]>0) {
							for(int dataindex = 0; dataindex < datanumber; dataindex++) {
								double neededBw = job.bandwidth[xindex][dataindex];
								int srcPos = (int) job.datapos[tindex][dataindex];
								if(bwOfSrcPos.containsKey(srcPos)) {
									double oldvalue = bwOfSrcPos.get(srcPos);
									bwOfSrcPos.put(srcPos, oldvalue + neededBw);
								}else {
									bwOfSrcPos.put(srcPos, 0 + neededBw);
								}
							}
							for(int posindex : bwOfSrcPos.keySet()) {
								if((tempUpArray[posindex]-bwOfSrcPos.get(posindex))<0) {
									resourceEnough = false;
									break;
								}else {
									tempUpArray[posindex]-=bwOfSrcPos.get(posindex);
								}
							}
						}
						
            		}
            		if(resourceEnough == false) {
            			break;
            		}
            	}
            	
            	if(resourceEnough == true) {
            	// if yes
            		// cut down the corresponding resource
            		for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
                		int taskId = job.unscheduledTaskList.get(tindex).getCloudletId();
                		int datanumber = job.data[tindex];
                		if(job.currentGreatePosition.containsKey(taskId)) {
                			int pos = job.currentGreatePosition.get(taskId);
                			int xindex = tindex * Parameters.numberOfDC + pos;
                			SlotArray[0][pos] -= 1;
                			preAssignedSlots -= 1;
    						// downlink
    						if(job.TotalTransferDataSize[xindex]>0) {
    							DownArray[0][pos] -= Parameters.bwBaselineOfDC[pos];
    						}
    						
    						// uplink
    						bwOfSrcPos = new HashMap<>();
    						if(job.TotalTransferDataSize[xindex]>0) {
    							for(int dataindex = 0; dataindex < datanumber; dataindex++) {
    								double neededBw = job.bandwidth[xindex][dataindex];
    								int srcPos = (int) job.datapos[tindex][dataindex];
    								if(bwOfSrcPos.containsKey(srcPos)) {
    									double oldvalue = bwOfSrcPos.get(srcPos);
    									bwOfSrcPos.put(srcPos, oldvalue + neededBw);
    								}else {
    									bwOfSrcPos.put(srcPos, 0 + neededBw);
    								}
    							}
    							for(int posindex : bwOfSrcPos.keySet()) {
    								
    								UpArray[0][posindex]-=bwOfSrcPos.get(posindex);
    								
    							}
    						}
    						
                		}
                	}
            		
            		
    	        	// copy based on the great assignment and preAssigned slots
            		MWNumericArray xOrig = null;
            		MWNumericArray tasknum = null;
            		MWNumericArray dcnum = null;
            		MWNumericArray allRateMuArray = null;
            		MWNumericArray allRateSigmaArray = null;
            		MWNumericArray TotalTransferDataSize = null;
            		MWNumericArray data = null;
            		MWNumericArray datapos = null;
            		MWNumericArray bandwidth = null;
            		MWNumericArray Slot = null;
            		MWNumericArray Up = null;
            		MWNumericArray Down = null;
            		MWNumericArray uselessDCforTask = null;
            		MWNumericArray r = null;
            		MWNumericArray slotlimit = null;
            		Object[] result = null;	/* Stores the result */
            		MWNumericArray xAssign = null;	/* Location of minimal value */
            		MWNumericArray UpdatedSlot = null;	/* solvable flag */
            		MWNumericArray UpdatedUp = null;
            		MWNumericArray UpdatedDown = null;
            		double[] x = null;
            		double[] slot = null;
            		double[] up = null;
            		double[] down = null;
            		try {
						taskAssign = new TaskAssign();
						// initial variables
						int[] dims = new int[2];
						dims[0] = 1;
						dims[1] = 1;
						tasknum = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
						dcnum = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
						r = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						slotlimit = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
						dims[1] = unscheduledTaskNum;
						data = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
						dims[1] = Parameters.numberOfDC;
						Slot = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						Up = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						Down = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						dims[1] = vnum;
						xOrig = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						allRateMuArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						allRateSigmaArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						TotalTransferDataSize = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						uselessDCforTask = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						dims[0] = unscheduledTaskNum;
						dims[1] = Parameters.ubOfData;
						datapos = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						dims[0] = vnum;
						bandwidth = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						
						// assign values
						tasknum.set(1, unscheduledTaskNum);
						dcnum.set(1, Parameters.numberOfDC);
						r.set(1, Parameters.r);
						slotlimit.set(1, preAssignedSlots);
						
						int[] pos = new int[2];
						
						for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
							// data datapos
							pos[0] = 1;
							pos[1] = tindex + 1;
							data.set(pos, job.data[tindex]);
							for(int dataindex = 0; dataindex < Parameters.ubOfData; dataindex++) {
								pos[0] = tindex + 1;
								pos[1] = dataindex + 1;
								datapos.set(pos, job.datapos[tindex][dataindex]);
							}
							// xOrig allRateMu allRateSigma uselessDCforTask bandwidth
							for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
								int xindex = tindex * Parameters.numberOfDC + dcindex;
								pos[0] = 1;
								pos[1] = xindex + 1;
								xOrig.set(pos, job.greatX[xindex]);
								allRateMuArray.set(pos, job.allRateMuArray[0][xindex]);
								allRateSigmaArray.set(pos, job.allRateSigmaArray[0][xindex]);
								TotalTransferDataSize.set(pos, job.TotalTransferDataSize[xindex]);
								uselessDCforTask.set(pos, job.uselessDCforTask[xindex]);
								for(int dataindex = 0; dataindex < Parameters.ubOfData; dataindex++) {
									pos[0] = xindex + 1;
									pos[1] = dataindex + 1;
									bandwidth.set(pos, job.bandwidth[xindex][dataindex]);
								}
							}
						}
						
						// Slot Up Down
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							pos[0] = 1;
							pos[1] = dcindex + 1;
							Slot.set(pos, SlotArray[0][dcindex]);
							Up.set(pos, UpArray[0][dcindex]);
							Down.set(pos, DownArray[0][dcindex]);
						}
						
						switch(Parameters.copystrategy) {
						case 1:
							result = taskAssign.copyStrategy(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
							break;
						case 2:
							result = taskAssign.copyStrategy_optimizeExp_Traverse(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
							break;
						case 3:
							result = taskAssign.copyStrategy_optimizeAll_orderedRes(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
							break;
						case 4:
							result = taskAssign.copyStrategy_optimizeAll_Traverse(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
							break;
						default:
							break;
						}					
						xAssign = (MWNumericArray)result[0];
						UpdatedSlot = (MWNumericArray)result[1];
						UpdatedUp = (MWNumericArray)result[2];
						UpdatedDown = (MWNumericArray)result[3];
						
						x = xAssign.getDoubleData();
						slot = UpdatedSlot.getDoubleData();
						up = UpdatedUp.getDoubleData();
						down = UpdatedDown.getDoubleData();
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							SlotArray[0][dcindex] = slot[dcindex];
							UpArray[0][dcindex] = up[dcindex];
							DownArray[0][dcindex] = down[dcindex];
						}
						
						
						
						// Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
						Queue<Task> taskSubmitted = new LinkedList<>();
						// successful assignment
						for (int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
							Task task = job.unscheduledTaskList.get(tindex);
							boolean submitflag = false;
							for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
								if (x[tindex*Parameters.numberOfDC + dcindex] != 0) {
									if(submitflag == false) {
										submitflag = true;
									}
									int submittedNum = (int) x[tindex*Parameters.numberOfDC + dcindex];
									for(int copyindex = 0; copyindex < submittedNum; copyindex++) {
										Vm vm = idleTaskSlotsOfDC.get(dcindex + DCbase).remove();
										if (tasks.containsKey(task.getCloudletId())) {
											Task speculativeTask = new Task(task);
											cloneTask(speculativeTask,task);
											speculativeTask.setAssignmentDCId(dcindex + DCbase);
											speculativeTask.assignmentDCindex = dcindex;
											speculativeTask.setSpeculativeCopy(true);
											speculativeTasks.get(speculativeTask.getCloudletId()).add(speculativeTask);
											submitSpeculativeTask(speculativeTask, vm);
										} else {
											task.setAssignmentDCId(dcindex + DCbase);
											task.assignmentDCindex = dcindex;
											tasks.put(task.getCloudletId(), task);
											submitTask(task, vm);
											speculativeTasks.put(task.getCloudletId(), new LinkedList<>());
										}
									}
									
								}
							}
							if(submitflag == true) {
								taskSubmitted.add(task);
							}
						}
						
						// there to verify that modify the job 
						// JobFactory whether change the corresponding value
						// JobList whether change the corresponding value
						job.unscheduledTaskList.removeAll(taskSubmitted);
						int numOfScheduledTask = scheduledTaskOfJob.get(job.getCloudletId());
						scheduledTaskOfJob.put(job.getCloudletId(), (numOfScheduledTask+taskSubmitted.size()));
						//job.sortedflag = false;
						JobFactory.put(job.getCloudletId(), job);
						if(job.unscheduledTaskList.size() == 0) {
							scheduler.getScheduledList().add(job);
						}
						
					} catch (MWException e) {
						// TODO Auto-generated catch block
						System.out.println("Exception: "+e.toString());
						e.printStackTrace();
					}finally {
						MWNumericArray.disposeArray(xOrig);
						MWNumericArray.disposeArray(tasknum);
						MWNumericArray.disposeArray(dcnum);
						MWNumericArray.disposeArray(allRateMuArray);
						MWNumericArray.disposeArray(allRateSigmaArray);
						MWNumericArray.disposeArray(TotalTransferDataSize);
						MWNumericArray.disposeArray(data);
						MWNumericArray.disposeArray(datapos);
						MWNumericArray.disposeArray(bandwidth);
						MWNumericArray.disposeArray(Slot);
						MWNumericArray.disposeArray(Up);
						MWNumericArray.disposeArray(Down);
						MWNumericArray.disposeArray(uselessDCforTask);
						MWNumericArray.disposeArray(r);
						MWNumericArray.disposeArray(slotlimit);
						MWNumericArray.disposeArray(xAssign);
						MWNumericArray.disposeArray(UpdatedSlot);
						MWNumericArray.disposeArray(UpdatedUp);
						MWNumericArray.disposeArray(UpdatedDown);
						if(taskAssign != null)
							taskAssign.dispose();
					}
            		
            	}else {
            	// if no
    	        	// solve the assignment based on the current resource
            		IloCplex cplex = null;
            		
            		try {
						cplex = new IloCplex();
						int vnumplusone = 1 + unscheduledTaskNum*Parameters.numberOfDC;
						
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
							if(vindex == (vnumplusone-1)) {
								objvals[vindex] = 1;
							}else {
								objvals[vindex] = 0;
							}
							
						}
						cplex.addMaximize(cplex.scalProd(var, objvals));
						
						int constraintsNum = 2 * unscheduledTaskNum + 3 * Parameters.numberOfDC + job.uselessConstraintsNum;
						IloRange[] rng = new IloRange[constraintsNum];
						int constraintIndex = 0;
						// constraints
						// extra constraints about task
						for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
							IloNumExpr[] itermOfTask = new IloNumExpr[vnumplusone];
							for(int taskindex = 0; taskindex < unscheduledTaskNum; taskindex++) {
								for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
									int xindex = taskindex*Parameters.numberOfDC + dcindex;
									if(taskindex == tindex) {
										itermOfTask[xindex] = cplex.prod((job.allRateMuArray[0][xindex]
												+ Parameters.r * job.allRateSigmaArray[0][xindex]), var[xindex]);
									}else {
										itermOfTask[xindex] = cplex.prod(0.0, var[xindex]);
									}
								}
							}
							itermOfTask[vnumplusone-1] = cplex.prod(-1.0, var[vnumplusone-1]);
							rng[constraintIndex] = cplex.addGe(cplex.sum(itermOfTask), 0.0);
							constraintIndex++;
						}
						
						// each task has one execution among DCs
						for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
							IloNumExpr[] itermOfTask = new IloNumExpr[vnumplusone];
							for(int taskindex = 0; taskindex < unscheduledTaskNum; taskindex++) {
								for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
									int xindex = taskindex*Parameters.numberOfDC + dcindex;
									if(taskindex == tindex) {
										itermOfTask[xindex] = cplex.prod(1.0, var[xindex]);
									}else {
										itermOfTask[xindex] = cplex.prod(0.0, var[xindex]);
									}
								}
							}
							itermOfTask[vnumplusone-1] = cplex.prod(0.0, var[vnumplusone-1]);
							rng[constraintIndex] = cplex.addEq(cplex.sum(itermOfTask), 1.0);
							constraintIndex++;
						}
						
						// machine limitation
						for(int datacenterindex = 0; datacenterindex < Parameters.numberOfDC; datacenterindex++) {
							IloNumExpr[] itermOfTask = new IloNumExpr[vnumplusone];
							for(int taskindex = 0; taskindex < unscheduledTaskNum; taskindex++) {
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
							for(int taskindex = 0; taskindex < unscheduledTaskNum; taskindex++) {
								for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
									int xindex = taskindex*Parameters.numberOfDC + dcindex;
									double upsum = 0d;
									for(int dataindex = 0; dataindex < job.data[taskindex]; dataindex++) {
										if(job.datapos[taskindex][dataindex] == datacenterindex) {
											upsum += job.bandwidth[xindex][dataindex];
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
							for(int taskindex = 0; taskindex < unscheduledTaskNum; taskindex++) {
								for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
									int xindex = taskindex*Parameters.numberOfDC + dcindex;
									if(dcindex == datacenterindex) {
										double downsum = 0d;
										for(int dataindex = 0; dataindex < job.data[taskindex]; dataindex++) {
												downsum += job.bandwidth[xindex][dataindex];
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
							if(job.uselessDCforTask[xindex] == 0) {
								rng[constraintIndex] = cplex.addEq(cplex.prod(1.0, var[xindex]),0.0);
								constraintIndex++;
							}
						}
						
						
						if(cplex.solve()) {
							
							singlex = new double[vnum];
							double[] vresult = cplex.getValues(var);
							for(int vindex = 0; vindex < vnum; vindex++) {
								singlex[vindex] = vresult[vindex];
							}
							
							double[] slack = cplex.getSlacks(rng);
							System.out.println("Solution status = " + cplex.getStatus());
							
							//verify x
							
							for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
								boolean success = false;
								for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
									int xindex = tindex * Parameters.numberOfDC + dcindex;
									
									if(singlex[xindex] > 0 && success == false) {
										boolean inter_resourceEnough = true;
										// machines
										if((SlotArray[0][dcindex]-1)<0) {
											inter_resourceEnough = false;
										}
										
										// downlink
										if(job.TotalTransferDataSize[xindex]>0) {
											if((DownArray[0][dcindex]-Parameters.bwBaselineOfDC[dcindex])<0) {
												inter_resourceEnough = false;
											}
										}
										
										// uplink
										bwOfSrcPos = new HashMap<>();
										if(job.TotalTransferDataSize[xindex]>0) {
											for(int dataindex = 0; dataindex < Parameters.ubOfData; dataindex++) {
												double neededBw = job.bandwidth[xindex][dataindex];
												int srcPos = (int) job.datapos[tindex][dataindex];
												if(bwOfSrcPos.containsKey(srcPos)) {
													double oldvalue = bwOfSrcPos.get(srcPos);
													bwOfSrcPos.put(srcPos, oldvalue + neededBw);
												}else {
													bwOfSrcPos.put(srcPos, 0 + neededBw);
												}
											}
											for(int pos : bwOfSrcPos.keySet()) {
												if((UpArray[0][pos]-bwOfSrcPos.get(pos))<0) {
													inter_resourceEnough = false;
													break;
												}
											}
										}
										
										if(inter_resourceEnough == true) {
											success = true;
											singlex[xindex] = 1;
											allzeroflag = false;
											// cut down resource
											// machines
											SlotArray[0][dcindex] -= 1;
											
											// downlink
											if(job.TotalTransferDataSize[xindex]>0) {
												DownArray[0][dcindex] -= Parameters.bwBaselineOfDC[dcindex];
											}
											
											// uplink
											
											if(job.TotalTransferDataSize[xindex]>0) {
												for(int pos : bwOfSrcPos.keySet()) {
													UpArray[0][pos] -= bwOfSrcPos.get(pos);
												}
											}
										}else {
											singlex[xindex] = 0;
										}
										
									}else {
										singlex[xindex] = 0;
									}
								}
							}
							cplex.end();
							
						}else {
							// greedy assign for the tasks in the job as well as its copy
							// when there is some tasks do not be assigned then the copy is not needed
							// use the matlab jar
							
							singlex = new double[vnum];
							for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
								Task task = job.unscheduledTaskList.get(tindex);
								int taskId = task.getCloudletId();
								
								boolean success = true;
								int successDC = -1;
								for(Map.Entry<Integer, Double> iterm:job.sortedListOfTask.get(taskId)) {
									int dcindex = iterm.getKey();
									int xindex = tindex * Parameters.numberOfDC + dcindex;
									success = true;
									// when the dc is not too far
//									if(job.uselessDCforTask[xindex] != 0) {
										// verify that the resource is enough
										
										// machines
										if((SlotArray[0][dcindex]-1)<0) {
											success = false;
											continue;
										}
										
										// downlink
										if(job.TotalTransferDataSize[xindex]>0) {
											if((DownArray[0][dcindex]-Parameters.bwBaselineOfDC[dcindex])<0) {
												success = false;
												continue;
											}
										}
										
										// uplink
										bwOfSrcPos = new HashMap<>();
										if(job.TotalTransferDataSize[xindex]>0) {
											for(int dataindex = 0; dataindex < Parameters.ubOfData; dataindex++) {
												double neededBw = job.bandwidth[xindex][dataindex];
												int srcPos = (int) job.datapos[tindex][dataindex];
												if(bwOfSrcPos.containsKey(srcPos)) {
													double oldvalue = bwOfSrcPos.get(srcPos);
													bwOfSrcPos.put(srcPos, oldvalue + neededBw);
												}else {
													bwOfSrcPos.put(srcPos, 0 + neededBw);
												}
											}
											for(int pos : bwOfSrcPos.keySet()) {
												if((UpArray[0][pos]-bwOfSrcPos.get(pos))<0) {
													success = false;
													break;
												}
											}
										}
										if(success == true) {
											SlotArray[0][dcindex] -= 1;
											if(job.TotalTransferDataSize[xindex]>0) {
												DownArray[0][dcindex] -= Parameters.bwBaselineOfDC[dcindex];

											}
											for(int pos : bwOfSrcPos.keySet()) {
												UpArray[0][pos]-=bwOfSrcPos.get(pos);
											}
											successDC = dcindex;
											break;
										}
//									}
								}
								if(success == true) {
									
									// store the greatest assignment info in the job with the current resource
									int xindex = tindex * Parameters.numberOfDC + successDC;
									allzeroflag = false;
									singlex[xindex] = 1;
								}
							}
							
						}	
					} catch (IloException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            		
            		// judge whether x is all zero
            		double[] x = null;
            		if(allzeroflag == false) {
            			// copy based on the fresh assignment
                		MWNumericArray xOrig = null;
                		MWNumericArray tasknum = null;
                		MWNumericArray dcnum = null;
                		MWNumericArray allRateMuArray = null;
                		MWNumericArray allRateSigmaArray = null;
                		MWNumericArray TotalTransferDataSize = null;
                		MWNumericArray data = null;
                		MWNumericArray datapos = null;
                		MWNumericArray bandwidth = null;
                		MWNumericArray Slot = null;
                		MWNumericArray Up = null;
                		MWNumericArray Down = null;
                		MWNumericArray uselessDCforTask = null;
                		MWNumericArray r = null;
                		MWNumericArray slotlimit = null;
                		Object[] result = null;	/* Stores the result */
                		MWNumericArray xAssign = null;	/* Location of minimal value */
                		MWNumericArray UpdatedSlot = null;	/* solvable flag */
                		MWNumericArray UpdatedUp = null;
                		MWNumericArray UpdatedDown = null;
                		
                		double[] slot = null;
                		double[] up = null;
                		double[] down = null;
                		try {
    						taskAssign = new TaskAssign();
    						// initial variables
    						int[] dims = new int[2];
    						dims[0] = 1;
    						dims[1] = 1;
    						tasknum = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
    						dcnum = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
    						r = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						slotlimit = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
    						dims[1] = unscheduledTaskNum;
    						data = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
    						dims[1] = Parameters.numberOfDC;
    						Slot = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						Up = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						Down = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						dims[1] = vnum;
    						xOrig = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						allRateMuArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						allRateSigmaArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						TotalTransferDataSize = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						uselessDCforTask = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						dims[0] = unscheduledTaskNum;
    						dims[1] = Parameters.ubOfData;
    						datapos = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						dims[0] = vnum;
    						bandwidth = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
    						
    						// assign values
    						tasknum.set(1, unscheduledTaskNum);
    						dcnum.set(1, Parameters.numberOfDC);
    						r.set(1, Parameters.r);
    						slotlimit.set(1, preAssignedSlots);
    						
    						int[] pos = new int[2];
    						
    						for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
    							// data datapos
    							pos[0] = 1;
    							pos[1] = tindex + 1;
    							data.set(pos, job.data[tindex]);
    							for(int dataindex = 0; dataindex < Parameters.ubOfData; dataindex++) {
    								pos[0] = tindex + 1;
    								pos[1] = dataindex + 1;
    								datapos.set(pos, job.datapos[tindex][dataindex]);
    							}
    							// xOrig allRateMu allRateSigma uselessDCforTask bandwidth
    							for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
    								int xindex = tindex * Parameters.numberOfDC + dcindex;
    								pos[0] = 1;
    								pos[1] = xindex + 1;
    								xOrig.set(pos, singlex[xindex]);
    								allRateMuArray.set(pos, job.allRateMuArray[0][xindex]);
    								allRateSigmaArray.set(pos, job.allRateSigmaArray[0][xindex]);
    								uselessDCforTask.set(pos, job.uselessDCforTask[xindex]);
    								for(int dataindex = 0; dataindex < Parameters.ubOfData; dataindex++) {
    									pos[0] = xindex + 1;
    									pos[1] = dataindex + 1;
    									bandwidth.set(pos, job.bandwidth[xindex][dataindex]);
    								}
    							}
    						}
    						
    						// current Slot Up Down
    						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
    							pos[0] = 1;
    							pos[1] = dcindex + 1;
    							Slot.set(pos, SlotArray[0][dcindex]);
    							Up.set(pos, UpArray[0][dcindex]);
    							Down.set(pos, DownArray[0][dcindex]);
    						}
    						
    						switch(Parameters.copystrategy) {
    						case 1:
    							result = taskAssign.copyStrategy(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
    							break;
    						case 2:
    							result = taskAssign.copyStrategy_optimizeExp_Traverse(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
    							break;
    						case 3:
    							result = taskAssign.copyStrategy_optimizeAll_orderedRes(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
    							break;
    						case 4:
    							result = taskAssign.copyStrategy_optimizeAll_Traverse(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
    							break;
    						default:
    							break;
    						}
    						xAssign = (MWNumericArray)result[0];
    						UpdatedSlot = (MWNumericArray)result[1];
    						UpdatedUp = (MWNumericArray)result[2];
    						UpdatedDown = (MWNumericArray)result[3];
    						
    						x = xAssign.getDoubleData();
    						slot = UpdatedSlot.getDoubleData();
    						up = UpdatedUp.getDoubleData();
    						down = UpdatedDown.getDoubleData();
    						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
    							SlotArray[0][dcindex] = slot[dcindex];
    							UpArray[0][dcindex] = up[dcindex];
    							DownArray[0][dcindex] = down[dcindex];
    						}
    						
    						
    						// Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
    						Queue<Task> taskSubmitted = new LinkedList<>();
    						// successful assignment
    						
    						for (int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
    							Task task = job.unscheduledTaskList.get(tindex);
    							boolean submitflag = false;
    							for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
    								if (x[tindex*Parameters.numberOfDC + dcindex] != 0) {
    									if(submitflag == false) {
    										submitflag = true;
    									}
    									int submittedNum = (int) x[tindex*Parameters.numberOfDC + dcindex];
    									for(int copyindex = 0; copyindex < submittedNum; copyindex++) {
    										Vm vm = idleTaskSlotsOfDC.get(dcindex + DCbase).remove();
    										if (tasks.containsKey(task.getCloudletId())) {
    											Task speculativeTask = new Task(task);
    											cloneTask(speculativeTask,task);
    											speculativeTask.setAssignmentDCId(dcindex + DCbase);
    											speculativeTask.assignmentDCindex = dcindex;
    											speculativeTask.setSpeculativeCopy(true);
    											speculativeTasks.get(speculativeTask.getCloudletId()).add(speculativeTask);
    											submitSpeculativeTask(speculativeTask, vm);
    										} else {
    											task.setAssignmentDCId(dcindex + DCbase);
    											task.assignmentDCindex = dcindex;
    											tasks.put(task.getCloudletId(), task);
    											submitTask(task, vm);
    											speculativeTasks.put(task.getCloudletId(), new LinkedList<>());
    										}
    									}
    									
    								}
    							}
    							if(submitflag == true) {
    								taskSubmitted.add(task);
    							}
    						}
    						// there to verify that modify the job 
    						// JobFactory whether change the corresponding value
    						// JobList whether change the corresponding value
    						job.unscheduledTaskList.removeAll(taskSubmitted);
    						int numOfScheduledTask = scheduledTaskOfJob.get(job.getCloudletId());
    						scheduledTaskOfJob.put(job.getCloudletId(), (numOfScheduledTask+taskSubmitted.size()));
    						
    						//job.sortedflag = false;
    						JobFactory.put(job.getCloudletId(), job);
    						if(job.unscheduledTaskList.size() == 0) {
    							scheduler.getScheduledList().add(job);
    						}
    						
    					} catch (MWException e) {
    						// TODO Auto-generated catch block
    						System.out.println("Exception: "+e.toString());
    						e.printStackTrace();
    					}finally {
    						MWNumericArray.disposeArray(xOrig);
    						MWNumericArray.disposeArray(tasknum);
    						MWNumericArray.disposeArray(dcnum);
    						MWNumericArray.disposeArray(allRateMuArray);
    						MWNumericArray.disposeArray(allRateSigmaArray);
    						MWNumericArray.disposeArray(TotalTransferDataSize);
    						MWNumericArray.disposeArray(data);
    						MWNumericArray.disposeArray(datapos);
    						MWNumericArray.disposeArray(bandwidth);
    						MWNumericArray.disposeArray(Slot);
    						MWNumericArray.disposeArray(Up);
    						MWNumericArray.disposeArray(Down);
    						MWNumericArray.disposeArray(uselessDCforTask);
    						MWNumericArray.disposeArray(r);
    						MWNumericArray.disposeArray(slotlimit);
    						MWNumericArray.disposeArray(xAssign);
    						MWNumericArray.disposeArray(UpdatedSlot);
    						MWNumericArray.disposeArray(UpdatedUp);
    						MWNumericArray.disposeArray(UpdatedDown);
    						if(taskAssign != null)
    							taskAssign.dispose();
    					}
                		
            		}
            	}
            	
        	}else {
        	// if no
            	// greedy assign for the tasks one by one
				singlex = new double[vnum];
				for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
					Task task = job.unscheduledTaskList.get(tindex);
					int taskId = task.getCloudletId();
					
					boolean success = true;
					int successDC = -1;
					for(Map.Entry<Integer, Double> iterm:job.sortedListOfTask.get(taskId)) {
						int dcindex = iterm.getKey();
						int xindex = tindex * Parameters.numberOfDC + dcindex;
						success = true;
						// when the dc is not too far
//						if(job.uselessDCforTask[xindex] != 0) {
							// verify that the resource is enough
							
							// machines
							if((SlotArray[0][dcindex]-1)<0) {
								success = false;
								continue;
							}
							
							// downlink
							if(job.TotalTransferDataSize[xindex]>0) {
								if((DownArray[0][dcindex]-Parameters.bwBaselineOfDC[dcindex])<0) {
									success = false;
									continue;
								}
							}
							
							// uplink
							bwOfSrcPos = new HashMap<>();
							if(job.TotalTransferDataSize[xindex]>0) {
								for(int dataindex = 0; dataindex < Parameters.ubOfData; dataindex++) {
									double neededBw = job.bandwidth[xindex][dataindex];
									int srcPos = (int) job.datapos[tindex][dataindex];
									if(bwOfSrcPos.containsKey(srcPos)) {
										double oldvalue = bwOfSrcPos.get(srcPos);
										bwOfSrcPos.put(srcPos, oldvalue + neededBw);
									}else {
										bwOfSrcPos.put(srcPos, 0 + neededBw);
									}
								}
								for(int pos : bwOfSrcPos.keySet()) {
									if((UpArray[0][pos]-bwOfSrcPos.get(pos))<0) {
										success = false;
										break;
									}
								}
							}
							if(success == true) {
								SlotArray[0][dcindex] -= 1;
								if(job.TotalTransferDataSize[xindex]>0) {
									DownArray[0][dcindex] -= Parameters.bwBaselineOfDC[dcindex];

								}
								for(int pos : bwOfSrcPos.keySet()) {
									UpArray[0][pos]-=bwOfSrcPos.get(pos);
								}
								successDC = dcindex;
								break;
							}
//						}
					}
					if(success == true) {
						
						// store the greatest assignment info in the job with the current resource
						int xindex = tindex * Parameters.numberOfDC + successDC;
						allzeroflag = false;
						singlex[xindex] = 1;
					}
				}
				
				if(allzeroflag == false) {
					// copy
					MWNumericArray xOrig = null;
	        		MWNumericArray tasknum = null;
	        		MWNumericArray dcnum = null;
	        		MWNumericArray allRateMuArray = null;
	        		MWNumericArray allRateSigmaArray = null;
	        		MWNumericArray TotalTransferDataSize = null;
	        		MWNumericArray data = null;
	        		MWNumericArray datapos = null;
	        		MWNumericArray bandwidth = null;
	        		MWNumericArray Slot = null;
	        		MWNumericArray Up = null;
	        		MWNumericArray Down = null;
	        		MWNumericArray uselessDCforTask = null;
	        		MWNumericArray r = null;
	        		MWNumericArray slotlimit = null;
	        		Object[] result = null;	/* Stores the result */
	        		MWNumericArray xAssign = null;	/* Location of minimal value */
	        		MWNumericArray UpdatedSlot = null;	/* solvable flag */
	        		MWNumericArray UpdatedUp = null;
	        		MWNumericArray UpdatedDown = null;
	        		double[] x = null;
	        		double[] slot = null;
	        		double[] up = null;
	        		double[] down = null;
	        		try {
						taskAssign = new TaskAssign();
						// initial variables
						int[] dims = new int[2];
						dims[0] = 1;
						dims[1] = 1;
						tasknum = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
						dcnum = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
						r = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						slotlimit = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
						dims[1] = unscheduledTaskNum;
						data = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
						dims[1] = Parameters.numberOfDC;
						Slot = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						Up = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						Down = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						dims[1] = vnum;
						xOrig = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						allRateMuArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						allRateSigmaArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						TotalTransferDataSize = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						uselessDCforTask = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						dims[0] = unscheduledTaskNum;
						dims[1] = Parameters.ubOfData;
						datapos = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						dims[0] = vnum;
						bandwidth = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						
						// assign values
						tasknum.set(1, unscheduledTaskNum);
						dcnum.set(1, Parameters.numberOfDC);
						r.set(1, Parameters.r);
						slotlimit.set(1, preAssignedSlots);
						
						int[] pos = new int[2];
						
						for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
							// data datapos
							pos[0] = 1;
							pos[1] = tindex + 1;
							data.set(pos, job.data[tindex]);
							for(int dataindex = 0; dataindex < Parameters.ubOfData; dataindex++) {
								pos[0] = tindex + 1;
								pos[1] = dataindex + 1;
								datapos.set(pos, job.datapos[tindex][dataindex]);
							}
							// xOrig allRateMu allRateSigma uselessDCforTask bandwidth
							for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
								int xindex = tindex * Parameters.numberOfDC + dcindex;
								pos[0] = 1;
								pos[1] = xindex + 1;
								xOrig.set(pos, singlex[xindex]);
								allRateMuArray.set(pos, job.allRateMuArray[0][xindex]);
								allRateSigmaArray.set(pos, job.allRateSigmaArray[0][xindex]);
								uselessDCforTask.set(pos, job.uselessDCforTask[xindex]);
								for(int dataindex = 0; dataindex < Parameters.ubOfData; dataindex++) {
									pos[0] = xindex + 1;
									pos[1] = dataindex + 1;
									bandwidth.set(pos, job.bandwidth[xindex][dataindex]);
								}
							}
						}
						
						// current Slot Up Down
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							pos[0] = 1;
							pos[1] = dcindex + 1;
							Slot.set(pos, SlotArray[0][dcindex]);
							Up.set(pos, UpArray[0][dcindex]);
							Down.set(pos, DownArray[0][dcindex]);
						}
						switch(Parameters.copystrategy) {
						case 1:
							result = taskAssign.copyStrategy(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
							break;
						case 2:
							result = taskAssign.copyStrategy_optimizeExp_Traverse(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
							break;
						case 3:
							result = taskAssign.copyStrategy_optimizeAll_orderedRes(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
							break;
						case 4:
							result = taskAssign.copyStrategy_optimizeAll_Traverse(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit);
							break;
						default:
							break;
						}

						xAssign = (MWNumericArray)result[0];
						UpdatedSlot = (MWNumericArray)result[1];
						UpdatedUp = (MWNumericArray)result[2];
						UpdatedDown = (MWNumericArray)result[3];
						
						x = xAssign.getDoubleData();
						slot = UpdatedSlot.getDoubleData();
						up = UpdatedUp.getDoubleData();
						down = UpdatedDown.getDoubleData();
						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							SlotArray[0][dcindex] = slot[dcindex];
							UpArray[0][dcindex] = up[dcindex];
							DownArray[0][dcindex] = down[dcindex];
						}
						
						
						// Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
						Queue<Task> taskSubmitted = new LinkedList<>();
						// successful assignment
						
						for (int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
							Task task = job.unscheduledTaskList.get(tindex);
							boolean submitflag = false;
							for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
								if (x[tindex*Parameters.numberOfDC + dcindex] != 0) {
									if(submitflag == false) {
										submitflag = true;
									}
									int submittedNum = (int) x[tindex*Parameters.numberOfDC + dcindex];
									for(int copyindex = 0; copyindex < submittedNum; copyindex++) {
										Vm vm = idleTaskSlotsOfDC.get(dcindex + DCbase).remove();
										if (tasks.containsKey(task.getCloudletId())) {
											Task speculativeTask = new Task(task);
											cloneTask(speculativeTask,task);
											speculativeTask.setAssignmentDCId(dcindex + DCbase);
											speculativeTask.assignmentDCindex = dcindex;
											speculativeTask.setSpeculativeCopy(true);
											speculativeTasks.get(speculativeTask.getCloudletId()).add(speculativeTask);
											submitSpeculativeTask(speculativeTask, vm);
										} else {
											task.setAssignmentDCId(dcindex + DCbase);
											task.assignmentDCindex = dcindex;
											tasks.put(task.getCloudletId(), task);
											submitTask(task, vm);
											speculativeTasks.put(task.getCloudletId(), new LinkedList<>());
										}
									}
									
								}
							}
							if(submitflag == true) {
								taskSubmitted.add(task);
							}
						}
						// there to verify that modify the job 
						// JobFactory whether change the corresponding value
						// JobList whether change the corresponding value
						job.unscheduledTaskList.removeAll(taskSubmitted);
						int numOfScheduledTask = scheduledTaskOfJob.get(job.getCloudletId());
						scheduledTaskOfJob.put(job.getCloudletId(), (numOfScheduledTask+taskSubmitted.size()));
						
						//job.sortedflag = false;
						JobFactory.put(job.getCloudletId(), job);
						if(job.unscheduledTaskList.size() == 0) {
							scheduler.getScheduledList().add(job);
						}
						
					} catch (MWException e) {
						// TODO Auto-generated catch block
						System.out.println("Exception: "+e.toString());
						e.printStackTrace();
					}finally {
						MWNumericArray.disposeArray(xOrig);
						MWNumericArray.disposeArray(tasknum);
						MWNumericArray.disposeArray(dcnum);
						MWNumericArray.disposeArray(allRateMuArray);
						MWNumericArray.disposeArray(allRateSigmaArray);
						MWNumericArray.disposeArray(TotalTransferDataSize);
						MWNumericArray.disposeArray(data);
						MWNumericArray.disposeArray(datapos);
						MWNumericArray.disposeArray(bandwidth);
						MWNumericArray.disposeArray(Slot);
						MWNumericArray.disposeArray(Up);
						MWNumericArray.disposeArray(Down);
						MWNumericArray.disposeArray(uselessDCforTask);
						MWNumericArray.disposeArray(r);
						MWNumericArray.disposeArray(slotlimit);
						MWNumericArray.disposeArray(xAssign);
						MWNumericArray.disposeArray(UpdatedSlot);
						MWNumericArray.disposeArray(UpdatedUp);
						MWNumericArray.disposeArray(UpdatedDown);
						if(taskAssign != null)
							taskAssign.dispose();
					}
				}
				
        		
        	}	
        }
        getCloudletList().removeAll(scheduler.getScheduledList());
        for(int sindex = 0; sindex < scheduler.getScheduledList().size(); sindex++) {
        	Job job = (Job)scheduler.getScheduledList().get(sindex);
        	if(!getCloudletSubmittedList().contains(job)) {
        		getCloudletSubmittedList().add(job);
        		cloudletsSubmitted += 1;
        	}
        	
        }
        
        
        
        
////        for (Cloudlet cloudlet : rankedList) {
//        	
//    	// execute the algorithm assign tasks
//    	// if the resource is not enough for all the jobs then delete the last one in rankedList
//    	int loopNumber = rankedList.size();
//    	for (int loopindex = 0; loopindex < loopNumber; loopindex++) {
//    		// add all the tasks in each job into TaskQueue
//    		for (int jobindex = 0; jobindex < (rankedList.size() - loopindex); jobindex++) {
//    			Job job = (Job)rankedList.get(jobindex);
//    			
//    			List<Task> tasklist = job.getTaskList();
//    			if(tasklist.size() == 0) {
//    				job.jobId = job.getCloudletId();
//    				taskReady(job);
//    			}else {
//    				for (int taskindex = 0; taskindex < tasklist.size();taskindex++) {
//        				Task task = tasklist.get(taskindex);
//        				taskReady(task);
//        			}
//    			}
//    			
//    		}
//    		if (tasksRemaining()) {
//    			int isResourceEnough = submitTasks();
//        		if (isResourceEnough > 0) {
//        			for (int jobindex = 0; jobindex < (rankedList.size() - loopindex); jobindex++) {
//            			Job job = (Job)rankedList.get(jobindex);
//            			
//            			
//            			if(!taskOfJob.containsKey(job.getCloudletId())) {
//            				if(job.getTaskList().size() == 0) {
//            					job.jobId = job.getCloudletId();
//                				taskOfJob.put(job.getCloudletId(), 1);
//                				ackTaskOfJob.put(job.getCloudletId(), 0);
//                				JobFactory.put(job.getCloudletId(), job);
//                			}else {
//                				// when return remember to delete the item in the three tables
//                				taskOfJob.put(job.getCloudletId(), job.getTaskList().size());
//                				ackTaskOfJob.put(job.getCloudletId(), 0);
//                				JobFactory.put(job.getCloudletId(), job);
//                			}
//            				
//            			}
//            			scheduler.getScheduledList().add(job);
//        			}
//        			break;
//        		}else {
//        			getTaskQueue().clear();
//        		}
//    		}
//    	}
//        	
////        }
//        getCloudletList().removeAll(scheduler.getScheduledList());
//        getCloudletSubmittedList().addAll(scheduler.getScheduledList());
//        cloudletsSubmitted += scheduler.getScheduledList().size();
    }
    
    
    private void cloneTask(Task speculativeTask, Task task) {
		speculativeTask.setDepth(task.getDepth());
		speculativeTask.setImpact(task.getImpact());
		speculativeTask.numberOfData = task.numberOfData;
		speculativeTask.jobId = task.jobId;
		speculativeTask.setSpeculativeCopy(true);
		// initial
		speculativeTask.positionOfData = new int[speculativeTask.numberOfData];
		speculativeTask.sizeOfData = new int[speculativeTask.numberOfData];
		speculativeTask.requiredBandwidth = new double[speculativeTask.numberOfData];
		speculativeTask.positionOfDataID = new int[speculativeTask.numberOfData];
		for(int dataindex = 0; dataindex < speculativeTask.numberOfData; dataindex++) {
			speculativeTask.positionOfData[dataindex] = task.positionOfData[dataindex];
			speculativeTask.sizeOfData[dataindex] = task.sizeOfData[dataindex];
			speculativeTask.requiredBandwidth[dataindex] = task.requiredBandwidth[dataindex];
			speculativeTask.positionOfDataID[dataindex] = task.positionOfDataID[dataindex];
		}
		speculativeTask.numberOfTransferData = new int[Parameters.numberOfDC];
		speculativeTask.TotalTransferDataSize = new double[Parameters.numberOfDC];
		speculativeTask.transferDataSize = new double[Parameters.numberOfDC][Parameters.ubOfData];
		for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
			speculativeTask.numberOfTransferData[dcindex] = task.numberOfTransferData[dcindex];
			speculativeTask.TotalTransferDataSize[dcindex] = task.TotalTransferDataSize[dcindex];
			for(int dataindex = 0; dataindex < speculativeTask.numberOfData; dataindex++) {
				speculativeTask.transferDataSize[dcindex][dataindex] = task.transferDataSize[dcindex][dataindex];
			}
		}
		speculativeTask.addChildList(task.getChildList());
		speculativeTask.addParentList(task.getParentList());
		List<FileItem> fileList = task.getFileList();
		for(int index = 0; index < fileList.size(); index++) {
			speculativeTask.getFileList().add(fileList.get(index));
		}
		
		
	}

	public Map<Integer, LinkedList<Vm>> getIdleTaskSlotsOfDC(){
		return idleTaskSlotsOfDC;
	}
    
    public double getRuntime() {
		return runtime;
	}


	private void submitTask(Task task, Vm vm) {
		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
				+ vm.getId() + " starts executing Task # "
				+ task.getCloudletId() + " \"" + task.getName() + " "
				+ task.getParams() + " \"");
		task.setVmId(vm.getId());
		if (Parameters.numGen.nextDouble() < getLikelihoodOfFailureOfDC().get(task.getAssignmentDCId())) {
			task.setScheduledToFail(true);
			task.setCloudletLength((long) (task.getCloudletLength() * getRuntimeFactorIncaseOfFailureOfDC().get(task.getAssignmentDCId())));
		} else {
			task.setScheduledToFail(false);
		}
		sendNow(getVmsToDatacentersMap().get(vm.getId()),
				CloudSimTags.CLOUDLET_SUBMIT_ACK, task);
	}

	private void submitSpeculativeTask(Task task, Vm vm) {
		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
				+ vm.getId() + " starts executing speculative copy of Task # "
				+ task.getCloudletId() + " \"" + task.getName() + " "
				+ task.getParams() + " \"");
		task.setVmId(vm.getId());
		if (Parameters.numGen.nextDouble() < getLikelihoodOfFailureOfDC().get(task.getAssignmentDCId())) {
			task.setScheduledToFail(true);
			task.setCloudletLength((long) (task.getCloudletLength() * getRuntimeFactorIncaseOfFailureOfDC().get(task.getAssignmentDCId())));
		} else {
			task.setScheduledToFail(false);
		}
		sendNow(getVmsToDatacentersMap().get(vm.getId()),
				CloudSimTags.CLOUDLET_SUBMIT_ACK, task);
	}
    
    
    
    // convert the string to exeline
    public static Object convertToCode(String jexlExp,Map<String,Object> map){    
        JexlEngine jexl=new JexlEngine();    
        Expression e = jexl.createExpression(jexlExp);    
        JexlContext jc = new MapContext();    
        for(String key:map.keySet()){    
            jc.set(key, map.get(key));    
        }    
        if(null==e.evaluate(jc)){    
            return "";    
        }    
        return e.evaluate(jc);    
    }    
    
    
//	protected int submitTasks() {
//		// Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
//		Queue<Task> taskSubmitted = new LinkedList<>();
//		// compute the task assignment among datacenters for ready tasks
//		Integer numberOfTask = getTaskQueue().size();
//		
//		int tasknum = numberOfTask;
//		int dcnum = Parameters.numberOfDC;
//		int iteration_bound = Parameters.boundOfIter;
//		double[][] probArray = new double[Parameters.numberOfDC][4];
//		double[][] allDuraArray = new double[numberOfTask*Parameters.numberOfDC][4];
//		int[] data = new int[numberOfTask];
//		double[][] datapos = new double[numberOfTask][Parameters.ubOfData];
//		double[][] bandwidth = new double[numberOfTask*Parameters.numberOfDC][Parameters.ubOfData];
//		double[][] SlotArray = new double[1][Parameters.numberOfDC];
//		double[][] UpArray = new double[1][Parameters.numberOfDC];
//		double[][] DownArray = new double[1][Parameters.numberOfDC];
//		double[] xb = null;
//		int flagi = 0;
//		LinkedList<Task> ReadyTasks = (LinkedList<Task>)getTaskQueue();
//		
//		//probArray
//		for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
//			for (int iterm = 0; iterm < 4; iterm++) {
//				double value = 0d;
//				switch (iterm) {
//				case 0:
//					value = (1-Parameters.likelihoodOfDCFailure[dcindex])*(1-Parameters.likelihoodOfFailure[dcindex])*(1-Parameters.likelihoodOfStragglerOfDC[dcindex]);
//					break;
//				case 1:
//					value = (1-Parameters.likelihoodOfDCFailure[dcindex])*(1-Parameters.likelihoodOfFailure[dcindex])*Parameters.likelihoodOfStragglerOfDC[dcindex];
//					break;
//				case 2:
//					value = (1-Parameters.likelihoodOfDCFailure[dcindex])*Parameters.likelihoodOfFailure[dcindex];
//					break;
//				case 3:
//					value = Parameters.likelihoodOfDCFailure[dcindex];
//					break;
//				default:
//					break;
//				}
//				probArray[dcindex][iterm] = value;
//			}
//		}
//		
//		//data datapos 
//		double[] Totaldatasize = new double[numberOfTask];
//		double[][] datasize = new double[numberOfTask][Parameters.ubOfData];
//		
//		for (int tindex = 0; tindex < numberOfTask; tindex++) {
//			Task task = ReadyTasks.get(tindex);
//			data[tindex] = task.numberOfData;
//			Totaldatasize[tindex] = 0d;
//			
//			for(int dataindex = 0; dataindex < task.numberOfData; dataindex++) {
//				datapos[tindex][dataindex] = task.positionOfData[dataindex];
//				task.positionOfDataID[dataindex] = task.positionOfData[dataindex] + DCbase;
//				datasize[tindex][dataindex] = task.sizeOfData[dataindex];
//				Totaldatasize[tindex] += task.sizeOfData[dataindex];
//			}
//			ReadyTasks.set(tindex, task);
//		}
//		
//		//bandwidth allDuraArray
//		for (int tindex = 0; tindex < numberOfTask; tindex++) {
//			Task task = ReadyTasks.get(tindex);
//			for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
//				int xindex = tindex*Parameters.numberOfDC + dcindex;
//				int numberOfTransferData = 0;
//				int datanumber = (int)data[tindex];
//				double[] datasizeOfTask = datasize[tindex];
//				double TotaldatasizeOfTask = Totaldatasize[tindex];
//				for(int dataindex = 0; dataindex < datanumber; dataindex++) {
//					if (datapos[tindex][dataindex] == dcindex) {
//						TotaldatasizeOfTask -= datasizeOfTask[dataindex];
//						datasizeOfTask[dataindex] = 0;
//					}else {
//						numberOfTransferData++;
//					}
//				} 
//				if(datanumber > 0) {
//					task.numberOfTransferData[dcindex] = numberOfTransferData;
//				}
//				
//				for(int dataindex = 0; dataindex < datanumber; dataindex++) {
//					if (TotaldatasizeOfTask > 0) {
//						bandwidth[xindex][dataindex] = Parameters.bwBaselineOfDC[dcindex]*datasizeOfTask[dataindex]/TotaldatasizeOfTask;
//						
//					}else {
//						bandwidth[xindex][dataindex] = 0;
//					}
//				}
//				for (int iterm = 0; iterm < 4; iterm++) {
//					double value = 0d;
//					switch (iterm) {
//					case 0:
//						value = task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
//								+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
//								+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
//						break;
//					case 1:
//						value = task.getMi()/(Parameters.MIPSbaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex])
//								+ TotaldatasizeOfTask/(Parameters.bwBaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex])
//								+ 2*task.getIo()/(Parameters.ioBaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex]);
//						break;
//					case 2:
//						value = (Parameters.runtimeFactorInCaseOfFailure[dcindex] + 1)
//								* task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
//								+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
//								+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
//						break;
//					case 3:
//						value = 2
//								* task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
//								+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
//								+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
//						break;
//					default:
//						break;
//					}
//					allDuraArray[xindex][iterm] = value;
//				}
//			}
//			ReadyTasks.set(tindex, task);
//			
//		}
//		
//		//SlotArray UpArray DownArray
//		
//		for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
//			if(healthyStateOfDC.get(dcindex + DCbase) == true) {
//				SlotArray[0][dcindex] = idleTaskSlotsOfDC.get(dcindex + DCbase).size();
//			}else {
//				SlotArray[0][dcindex] = 0;
//			}
//			UpArray[0][dcindex] = getUplinkOfDC().get(dcindex + DCbase);
//			DownArray[0][dcindex] = getDownlinkOfDC().get(dcindex + DCbase);
//		}
//		
//		try {
//			
//			MatlabTypeConverter processor = new MatlabTypeConverter(proxy);
//			processor.setNumericArray("probArray", new MatlabNumericArray(probArray, null));
//			processor.setNumericArray("allDuraArray", new MatlabNumericArray(allDuraArray, null));
//			processor.setNumericArray("bandwidth", new MatlabNumericArray(bandwidth,null));
//			processor.setNumericArray("SlotArray", new MatlabNumericArray(SlotArray, null));
//			processor.setNumericArray("UpArray", new MatlabNumericArray(UpArray, null));
//			processor.setNumericArray("DownArray", new MatlabNumericArray(DownArray,null));
//			processor.setNumericArray("datapos", new MatlabNumericArray(datapos, null));
//			proxy.setVariable("tasknum", tasknum);
//			proxy.setVariable("dcnum", dcnum);
//			proxy.setVariable("iteration_bound", iteration_bound);
//			proxy.setVariable("data", data);
//			
//			
//		//	proxy.eval("[x,flag] = command(tasknum,dcnum,probArray,allDuraArray,data,datapos,bandwidth,SlotArray,UpArray,DownArray,iteration_bound);");
//			proxy.eval("[x,flag] = commandwithnocopy(tasknum,dcnum,probArray,allDuraArray,data,datapos,bandwidth,SlotArray,UpArray,DownArray,iteration_bound);");
//			
//			xb = (double[])proxy.getVariable("x");
//			flagi = (int)((double[])proxy.getVariable("flag"))[0];
//			
//		} catch (MatlabInvocationException e) {
//			e.printStackTrace();
//		}
//		
//		
//		
//		
//		if (flagi > 0) {
//			// successful assignment
//			for (int tindex = 0; tindex < numberOfTask; tindex++) {
//				Task task = ReadyTasks.get(tindex);
//				for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
//					if (xb[tindex*Parameters.numberOfDC + dcindex] == 1) {
//						Vm vm = idleTaskSlotsOfDC.get(dcindex + DCbase).remove();
//						if (tasks.containsKey(task.getCloudletId())) {
//							Task speculativeTask = new Task(task);
//							speculativeTask.setAssignmentDCId(dcindex + DCbase);
//							speculativeTask.assignmentDCindex = dcindex;
//							speculativeTask.setSpeculativeCopy(true);
//							speculativeTasks.get(speculativeTask.getCloudletId()).add(speculativeTask);
//							submitSpeculativeTask(speculativeTask, vm);
//						} else {
//							task.setAssignmentDCId(dcindex + DCbase);
//							task.assignmentDCindex = dcindex;
//							tasks.put(task.getCloudletId(), task);
//							submitTask(task, vm);
//							speculativeTasks.put(task.getCloudletId(), new LinkedList<>());
//						}
//					}
//				}
//				taskSubmitted.add(task);
//			}
//			getTaskQueue().removeAll(taskSubmitted);
//		}
//		
//		return flagi;
//		
//	}
//    
//    
    
    
    
    
    

    /**
     * Process a cloudlet (job) return event.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    @Override
    protected void processCloudletReturn(SimEvent ev) {
    	Task task = (Task) ev.getData();
		Vm vm = vms.get(task.getVmId());
		Host host = vm.getHost();
		if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
			// in case that speculative and original tasks complete at the same time
			if(!tasks.containsKey(task.getCloudletId())) {
				return;
			}
			Task originalTask = tasks.remove(task.getCloudletId());
			
			LinkedList<Task> speculativeTaskOfTask = (LinkedList<Task>)speculativeTasks
					.remove(task.getCloudletId());

			if ((task.isSpeculativeCopy() && speculativeTaskOfTask.size() == 0)
					|| originalTask == null) {
				return;
			}

			if (task.isSpeculativeCopy()) {
				
				Iterator<Task> it = speculativeTaskOfTask.iterator();
				while(it.hasNext()) {
					Task speculativeTask = it.next();
					if (speculativeTask.getVmId() == task.getVmId()) {
						Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
								+ speculativeTask.getVmId()
								+ " completed speculative copy of Task # "
								+ speculativeTask.getCloudletId() + " \""
								+ speculativeTask.getName() + " "
								+ speculativeTask.getParams() + " \"");
					} else {
						Log.printLine(CloudSim.clock() + ": " + getName()
						+ ": VM # " + speculativeTask.getVmId()
						+ " cancelled speculative copy of Task # "
						+ speculativeTask.getCloudletId() + " \""
						+ speculativeTask.getName() + " "
						+ speculativeTask.getParams() + " \"");
						vms.get(speculativeTask.getVmId()).getCloudletScheduler()
						.cloudletCancel(speculativeTask.getCloudletId());
					}
					
				}
				
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
						+ originalTask.getVmId() + " cancelled Task # "
						+ originalTask.getCloudletId() + " \""
						+ originalTask.getName() + " "
						+ originalTask.getParams() + " \"");
				vms.get(originalTask.getVmId()).getCloudletScheduler()
						.cloudletCancel(originalTask.getCloudletId());
			} else {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
						+ originalTask.getVmId() + " completed Task # "
						+ originalTask.getCloudletId() + " \""
						+ originalTask.getName() + " "
						+ originalTask.getParams() + " \"");
				if (speculativeTaskOfTask.size() != 0) {
					
					Iterator<Task> it = speculativeTaskOfTask.iterator();
					while(it.hasNext()) {
						Task speculativeTask = it.next();
						Log.printLine(CloudSim.clock() + ": " + getName()
						+ ": VM # " + speculativeTask.getVmId()
						+ " cancelled speculative copy of Task # "
						+ speculativeTask.getCloudletId() + " \""
						+ speculativeTask.getName() + " "
						+ speculativeTask.getParams() + " \"");
						vms.get(speculativeTask.getVmId()).getCloudletScheduler()
						.cloudletCancel(speculativeTask.getCloudletId());
					}
					
				}
			}

			// free task slots occupied by finished / cancelled tasks
			Vm originalVm = vms.get(originalTask.getVmId());
			idleTaskSlotsOfDC.get(originalVm.DCId).add(originalVm);
			// return bandwidth
			double Totaldown = 0;
			if(originalTask.numberOfData > 0) {
				for(int dataindex = 0; dataindex < originalTask.numberOfData; dataindex++ ) {
					Totaldown += originalTask.requiredBandwidth[dataindex];
					if (originalTask.requiredBandwidth[dataindex] > 0) {
						sendNow(originalTask.positionOfDataID[dataindex], CloudSimTags.UPLINK_RETURN,originalTask.requiredBandwidth[dataindex]);
						double upbandwidth = getUplinkOfDC().get(originalTask.positionOfDataID[dataindex]) + originalTask.requiredBandwidth[dataindex];
						getUplinkOfDC().put(originalTask.positionOfDataID[dataindex], upbandwidth);
					}
					
				}
				if (Totaldown > 0) {
					sendNow(originalTask.getAssignmentDCId(), CloudSimTags.DOWNLINK_RETURN, Totaldown);
					double downbandwidth = getDownlinkOfDC().get(originalTask.getAssignmentDCId()) + Totaldown;
					getDownlinkOfDC().put(originalTask.getAssignmentDCId(), downbandwidth);
				}
				
			}
			//tasks.remove(originalTask.getCloudletId());
			//taskSucceeded(originalTask, originalVm);
			if (speculativeTaskOfTask.size() != 0) {
				Iterator<Task> it = speculativeTaskOfTask.iterator();
				while(it.hasNext()) {
					Task speculativeTask = it.next();
					Vm speculativeVm = vms.get(speculativeTask.getVmId());
					idleTaskSlotsOfDC.get(speculativeVm.DCId).add(speculativeVm);
					// return bandwidth
					Totaldown = 0;
					if(originalTask.numberOfData > 0) {
						for(int dataindex = 0; dataindex < originalTask.numberOfData; dataindex++ ) {
							Totaldown += speculativeTask.requiredBandwidth[dataindex];
							if (speculativeTask.requiredBandwidth[dataindex] > 0) {
								sendNow(speculativeTask.positionOfDataID[dataindex], CloudSimTags.UPLINK_RETURN,speculativeTask.requiredBandwidth[dataindex]);
								double upbandwidth = getUplinkOfDC().get(speculativeTask.positionOfDataID[dataindex]) + speculativeTask.requiredBandwidth[dataindex];
								getUplinkOfDC().put(speculativeTask.positionOfDataID[dataindex], upbandwidth);
							}
							
						}
						if(Totaldown > 0) {
							sendNow(speculativeTask.getAssignmentDCId(), CloudSimTags.DOWNLINK_RETURN, Totaldown);
							double downbandwidth = getDownlinkOfDC().get(speculativeTask.getAssignmentDCId()) + Totaldown;
							getDownlinkOfDC().put(speculativeTask.getAssignmentDCId(), downbandwidth);
						}
						
					}
					
					
					//taskFailed(speculativeTask, speculativeVm);
				}
				//speculativeTasks.remove(originalTask.getCloudletId());
			}
			
			collectAckTask(task);

			// if the task finished unsuccessfully,
			// if it was a speculative task, just leave it be
			// otherwise, -- if it exists -- the speculative task becomes the
			// new original
		}else {
			LinkedList<Task> speculativeTaskOfTask = (LinkedList<Task>)speculativeTasks.remove(task.getCloudletId());
			if (task.isSpeculativeCopy()) {
				Log.printLine(CloudSim.clock()
						+ ": "
						+ getName()
						+ ": VM # "
						+ task.getVmId()
						+ " encountered an error with speculative copy of Task # "
						+ task.getCloudletId() + " \"" + task.getName() + " "
						+ task.getParams() + " \"");
				for(int index = 0; index < speculativeTaskOfTask.size(); index++ ) {
					if (speculativeTaskOfTask.get(index).getVmId() == task.getVmId()) {
						Task speculativeTask = speculativeTaskOfTask.get(index);
						double Totaldown = 0;
						if(speculativeTask.numberOfData > 0) {
							for(int dataindex = 0; dataindex < speculativeTask.numberOfData; dataindex++ ) {
								Totaldown += speculativeTask.requiredBandwidth[dataindex];
								if (speculativeTask.requiredBandwidth[dataindex] > 0) {
									sendNow(speculativeTask.positionOfDataID[dataindex], CloudSimTags.UPLINK_RETURN,speculativeTask.requiredBandwidth[dataindex]);
									double upbandwidth = getUplinkOfDC().get(speculativeTask.positionOfDataID[dataindex]) + speculativeTask.requiredBandwidth[dataindex];
									getUplinkOfDC().put(speculativeTask.positionOfDataID[dataindex], upbandwidth);
								}
								
							}
							if(Totaldown > 0) {
								sendNow(speculativeTask.getAssignmentDCId(), CloudSimTags.DOWNLINK_RETURN, Totaldown);
								double downbandwidth = getDownlinkOfDC().get(speculativeTask.getAssignmentDCId()) + Totaldown;
								getDownlinkOfDC().put(speculativeTask.getAssignmentDCId(), downbandwidth);
							}
							
						}
						speculativeTaskOfTask.remove(index);
						break;
					}
				}
				// delete the fail speculative task from the speculativelist
				speculativeTasks.put(task.getCloudletId(), speculativeTaskOfTask);
				
			} else {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
						+ task.getVmId() + " encountered an error with Task # "
						+ task.getCloudletId() + " \"" + task.getName() + " "
						+ task.getParams() + " \"");
				
				
				
				Task originalTask = tasks.remove(task.getCloudletId());
				double Totaldown = 0;
				if(originalTask.numberOfData > 0) {
					for(int dataindex = 0; dataindex < originalTask.numberOfData; dataindex++ ) {
						Totaldown += originalTask.requiredBandwidth[dataindex];
						if (originalTask.requiredBandwidth[dataindex] > 0) {
							sendNow(originalTask.positionOfDataID[dataindex], CloudSimTags.UPLINK_RETURN,originalTask.requiredBandwidth[dataindex]);
							double upbandwidth = getUplinkOfDC().get(originalTask.positionOfDataID[dataindex]) + originalTask.requiredBandwidth[dataindex];
							getUplinkOfDC().put(originalTask.positionOfDataID[dataindex], upbandwidth);
						}
						
					}
					if(Totaldown > 0) {
						sendNow(originalTask.getAssignmentDCId(), CloudSimTags.DOWNLINK_RETURN, Totaldown);
						double downbandwidth = getDownlinkOfDC().get(originalTask.getAssignmentDCId()) + Totaldown;
						getDownlinkOfDC().put(originalTask.getAssignmentDCId(), downbandwidth);
					}
				}
				if (speculativeTaskOfTask.size() != 0) {
					Task speculativeTask = speculativeTaskOfTask.remove();
					
					speculativeTask.setSpeculativeCopy(false);
					
					tasks.put(speculativeTask.getCloudletId(), speculativeTask);
					speculativeTasks.put(speculativeTask.getCloudletId(), speculativeTaskOfTask);
				} else {
					
					collectAckTask(task);
				}
			}
			idleTaskSlotsOfDC.get(vm.DCId).add(vm);
		}
    }
    
    protected void resetTask(Task task) {
		task.setCloudletFinishedSoFar(0);
		try {
			double taskExecStarttime = task.getExecStartTime();
			double taskFinishTime = task.getFinishTime();
			task.setCloudletStatus(Cloudlet.CREATED);
			task.setExecStartTime(taskExecStarttime);
			task.setFinishTime(taskFinishTime);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    

    private void collectAckTask(Task task) {
    	
    	// deal with failed task in this function
    	
    	
    	int attributedJobId = task.jobId;
    	Job job = JobFactory.get(attributedJobId);
    	if(task.getStatus() == Cloudlet.FAILED) {
    		resetTask(task);
    		if(job.unscheduledTaskList.size() > 0) {
    			int numOfScheduledTask = scheduledTaskOfJob.get(job.getCloudletId());
    			scheduledTaskOfJob.put(job.getCloudletId(), numOfScheduledTask - 1);
    			job.unscheduledTaskList.add(task);
    		}else {
    			int numOfScheduledTask = scheduledTaskOfJob.get(job.getCloudletId());
    			scheduledTaskOfJob.put(job.getCloudletId(), numOfScheduledTask - 1);
    			job.unscheduledTaskList.add(task);
    			getCloudletList().add(job);
    		}
    		schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
    		return ;
    	}
        int alreadyAckTaskNumber = ackTaskOfJob.get(attributedJobId);
        ackTaskOfJob.put(attributedJobId, alreadyAckTaskNumber + 1);
        
    	List<Task> originalTaskList = job.getTaskList();
    	for(int taskindex = 0; taskindex < originalTaskList.size(); taskindex++) {
    		if(originalTaskList.get(taskindex).getCloudletId() == task.getCloudletId()) {
    			originalTaskList.set(taskindex, task);
    			break;
    		}
    	}
    	//job.setTaskList(originalTaskList);
    	JobFactory.put(job.getCloudletId(), job);
    	if(ackTaskOfJob.get(attributedJobId).equals(scheduledTaskOfJob.get(attributedJobId))) {
    		//not really update right now, should wait 1 s until many jobs have returned
            schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
    	}
        if(ackTaskOfJob.get(attributedJobId).equals(taskOfJob.get(attributedJobId))) {
        	// modify the job state and return the job to the WorkflowEngine
        	Job completeJob = JobFactory.get(attributedJobId);
        	List<Task> completeTaskList = completeJob.getTaskList();
        	if(completeTaskList.size() == 0) {
        		getCloudletReceivedList().add(completeJob);
                getCloudletSubmittedList().remove(completeJob);
                if(completeJob.getStatus() == Cloudlet.SUCCESS) {
                	schedule(this.workflowEngineId, 0.0, CloudSimTags.CLOUDLET_RETURN, completeJob);

                    taskOfJob.remove(attributedJobId);
                    ackTaskOfJob.remove(attributedJobId);
                    scheduledTaskOfJob.remove(attributedJobId);
                    JobFactory.remove(attributedJobId);
                    
                    cloudletsSubmitted--;
                    //not really update right now, should wait 1 s until many jobs have returned
                    schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
                }else {
                	resetTask(completeJob);
                    schedule(this.workflowEngineId, 0.0, CloudSimTags.CLOUDLET_RETURN, completeJob);

                    taskOfJob.remove(attributedJobId);
                    scheduledTaskOfJob.remove(attributedJobId);
                    ackTaskOfJob.remove(attributedJobId);
                    JobFactory.remove(attributedJobId);
                    
                    cloudletsSubmitted--;
                    //not really update right now, should wait 1 s until many jobs have returned
                    schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
                }
                return ;
        	}
        	boolean successflag = true;
        	double JobExeStartTime = Double.MAX_VALUE;
        	if(completeJob.getExecStartTime() != -1) {
        		JobExeStartTime = completeJob.getExecStartTime();
        	}
        	double JobFinishedTime = Double.MIN_VALUE;
        	if(completeJob.getFinishTime() != -1) {
        		JobFinishedTime = completeJob.getFinishTime();
        	}
        	for (int taskindex = 0; taskindex < completeTaskList.size(); taskindex++) {
        		Task completeTask = completeTaskList.get(taskindex);
        		if(completeTask.getStatus() == Cloudlet.SUCCESS) {
        			double taskstarttime = completeTask.getExecStartTime();
            		double taskfinishtime = completeTask.getFinishTime();
            		if(taskstarttime < JobExeStartTime) {
            			JobExeStartTime = taskstarttime;
            		}
            		if(taskfinishtime > JobFinishedTime) {
            			JobFinishedTime = taskfinishtime;
            		}
            		completeJob.successTaskList.add(completeTask);
        		}else {
        			double taskstarttime = completeTask.getExecStartTime();
        			if(taskstarttime < JobExeStartTime) {
            			JobExeStartTime = taskstarttime;
            		}
        			resetTask(completeTask);
        			completeJob.getTaskList().set(taskindex, completeTask);
        		}
        		
//        		if (successflag == true && completeTask.getStatus() == Cloudlet.FAILED) {
//        			successflag = false;
//        			try {
//						completeJob.setCloudletStatus(Cloudlet.FAILED);
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//        		}
        		
        	}
        	if(successflag == true) {
        		try {
					completeJob.setCloudletStatus(Cloudlet.SUCCESS);
				} catch (Exception e) {
					e.printStackTrace();
				}
        	}
        	if(JobExeStartTime != Double.MAX_VALUE) {
        		completeJob.setExecStartTime(JobExeStartTime);
        	}
        	if(JobFinishedTime != Double.MIN_VALUE) {
        		completeJob.setFinishTime(JobFinishedTime);
        	}
        	
        	getCloudletReceivedList().add(completeJob);
            getCloudletSubmittedList().remove(completeJob);
            
            schedule(this.workflowEngineId, 0.0, CloudSimTags.CLOUDLET_RETURN, completeJob);

            taskOfJob.remove(attributedJobId);
            ackTaskOfJob.remove(attributedJobId);
            JobFactory.remove(attributedJobId);
            scheduledTaskOfJob.remove(attributedJobId);
            cloudletsSubmitted--;
            //not really update right now, should wait 1 s until many jobs have returned
            schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
        	
        }
        
	}

	/**
     * Start this entity (WorkflowScheduler)
     */
//    @Override
//    public void startEntity() {
//        Log.printLine(getName() + " is starting...");
//        // this resource should register to regional GIS.
//        // However, if not specified, then register to system GIS (the
//        // default CloudInformationService) entity.
//        //int gisID = CloudSim.getEntityId(regionalCisName);
//        int gisID = -1;
//        if (gisID == -1) {
//            gisID = CloudSim.getCloudInfoServiceEntityId();
//        }
//
//        // send the registration to GIS
//        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
//    }

    /**
     * Terminate this entity (WorkflowScheduler)
     */
    @Override
    public void shutdownEntity() {
        clearDatacenters();
        Log.printLine(getName() + " is shutting down...");
    }

    @Override
    protected void clearDatacenters() {
		for (Vm vm : getVmsCreatedList()) {
			if (vm instanceof DynamicVm) {
				DynamicVm dVm = (DynamicVm) vm;
				dVm.closePerformanceLog();
			}
		}
		super.clearDatacenters();
		runtime = CloudSim.clock();
	}
    
    
    
    
    public int getTaskSlotsPerVm() {
		return taskSlotsPerVm;
	}
    
    
    
    /**
     * Submit cloudlets (jobs) to the created VMs. Scheduling is here
     */
    @Override
    protected void submitCloudlets() {
    	for (Vm vm : getVmsCreatedList()) {
			vms.put(vm.getId(), vm);
			for (int i = 0; i < getTaskSlotsPerVm(); i++) {
				Integer dcId = getVmsToDatacentersMap().get(vm.getId());
				if (!getIdleTaskSlotsOfDC().containsKey(dcId)) {
					getIdleTaskSlotsOfDC().put(dcId, new LinkedList<>());
					getIdleTaskSlotsOfDC().get(dcId).add(vm);
				}else {
					getIdleTaskSlotsOfDC().get(dcId).add(vm);
				}
				//idleTaskSlots.add(vm);
			}
		}
        sendNow(this.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, null);
    }
    /**
     * A trick here. Assure that we just submit it once
     */
    private boolean processCloudletSubmitHasShown = false;

    /**
     * Submits cloudlet (job) list
     *
     * @param ev a simEvent object
     */
    @Override
    protected void processCloudletSubmit(SimEvent ev) {
        List<Job> list = (List) ev.getData();
        getCloudletList().addAll(list);

        sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);
        if (!processCloudletSubmitHasShown) {
            processCloudletSubmitHasShown = true;
        }
    }

    /**
     * Process a request for the characteristics of a PowerDatacenter.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    @Override
    protected void processResourceCharacteristicsRequest(SimEvent ev) {
		setDatacenterIdsList(CloudSim.getCloudResourceList());
		setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());

		Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloud Resource List received with "
				+ getDatacenterIdsList().size() + " resource(s)");

		for (Integer datacenterId : getDatacenterIdsList()) {
			sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
		}
	}

	
	

}
