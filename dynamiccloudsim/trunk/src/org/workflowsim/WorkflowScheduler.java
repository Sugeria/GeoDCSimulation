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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import com.sun.org.apache.xalan.internal.xsltc.runtime.Parameter;

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
import org.apache.log4j.*;
import gurobi.*;
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

	
	// comparator
	public class TaskProgressRateComparator implements Comparator<Task> {

		@Override
		public int compare(Task task1, Task task2) {
			return Double.compare(progressRates.get(task1),
					progressRates.get(task2));
		}

	}

	public class TaskEstimatedTimeToCompletionComparator implements Comparator<Task> {

		@Override
		public int compare(Task task1, Task task2) {
			return Double.compare(estimatedTimesToCompletion.get(task1),
					estimatedTimesToCompletion.get(task2));
		}

	}
	
	
	
    
	public class VmSumOfProgressScoresComparator implements Comparator<Vm> {

		Map<Integer, Double> vmIdToSumOfProgressScores;

		public VmSumOfProgressScoresComparator(
				Map<Integer, Double> vmIdToSumOfProgressScores) {
			this.vmIdToSumOfProgressScores = vmIdToSumOfProgressScores;
		}

		@Override
		public int compare(Vm vm1, Vm vm2) {
			return Double.compare(vmIdToSumOfProgressScores.get(vm1.getId()),
					vmIdToSumOfProgressScores.get(vm2.getId()));
		}

	}
    
    
	
	
	
	
	
    /**
     * The workflow engine id associated with this workflow algorithm.
     */
    private int workflowEngineId;
    public Map<Integer, Vm> vms;
    public LinkedList<Task> taskQueue; 
    private int taskSlotsPerVm;
    // two collections of tasks, which are currently running;
 	// note that the second collections is a subset of the first collection
    public Map<Integer, Task> tasks;
    public Map<Integer, Queue<Task>> speculativeTasks;
    public Map<Integer, LinkedList<Vm>> idleTaskSlotsOfDC;
    public Map<Integer, Integer> taskOfJob;
    public Map<Integer, Integer> ackTaskOfJob;
    public Map<Integer, Integer> scheduledTaskOfJob;
    public Map<Integer, Job> JobFactory;
    public Map<Integer, Integer> usedSlotsOfJob;
	private double runtime;
	public int slotNum;
	
	
	public static final Logger log = Logger.getLogger(WorkflowScheduler.class);
	
	public Map<Integer, Integer> ori_idleTaskSlotsOfDC;
	
//	private MatlabProxyFactory factory;
//	public MatlabProxy proxy;

	


	// a node is slow if the sum of progress scores for all succeeded and
	// in-progress tasks on the node is below this threshold
	// speculative tasks are not executed on slow nodes
	// default: 25th percentile of node progress rates
	protected final double slowNodeThreshold = 0.1;
	protected double currentSlowNodeThreshold;

	// A threshold that a task's progress rate is compared with to determine
	// whether it is slow enough to be speculated upon
	// default: 25th percentile of task progress rates
	// 0.25
	protected final double slowTaskThreshold = 0.25;
	protected double currentSlowTaskThreshold;

	// a cap on the number of speculative tasks that can be running at once
	// (given as a percentage of task slots)
	// default: 10% of available task slots
	protected final double speculativeCap = 0.1;
	protected int speculativeCapAbs;

	protected Map<Vm, Double> nSucceededTasksPerVm;

	public Map<Task, Double> progressScores;
	public Map<Task, Double> timesTaskHasBeenRunning;
	public Map<Task, Double> progressRates;
	public Map<Task, Double> estimatedTimesToCompletion;
	
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
		ori_idleTaskSlotsOfDC = new HashMap<>();
		taskOfJob = new HashMap<>();
		ackTaskOfJob = new HashMap<>();
		scheduledTaskOfJob = new HashMap<>();
		JobFactory = new HashMap<>();
		usedSlotsOfJob = new HashMap<>();
		slotNum = 0;
//		nSucceededTasksPerVm = new HashMap<>();
		
		progressScores = new HashMap<>();
		timesTaskHasBeenRunning = new HashMap<>();
		progressRates = new HashMap<>();
		estimatedTimesToCompletion = new HashMap<>();
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
//		nSucceededTasksPerVm.put(vm, nSucceededTasksPerVm.get(vm)
//				+ progressScores.get(task));
	}
	
	public void taskFailed(Task task, Vm vm) {
		
//		nSucceededTasksPerVm.put(vm, nSucceededTasksPerVm.get(vm)
//				+ progressScores.get(task));
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
    	
    	
    	
//    if (CloudSim.clock() != lastProcessTime) 
    if(true){
    	
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
//        int slotNum = 0;
        
        
        List<Cloudlet> rankedList = scheduler.getRankedList();
        double[][] SlotArray = new double[1][Parameters.numberOfDC];
		double[][] UpArray = new double[1][Parameters.numberOfDC];
		double[][] DownArray = new double[1][Parameters.numberOfDC];
		
        //current SlotArray UpArray DownArray
		log.info("Resource");
		log.info(CloudSim.clock()+"\t");
  		for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
  			if(healthyStateOfDC.get(dcindex + DCbase) == true) {
  				SlotArray[0][dcindex] = idleTaskSlotsOfDC.get(dcindex + DCbase).size();
//  				slotNum += SlotArray[0][dcindex];
  			}else {
  				SlotArray[0][dcindex] = 0;
  			}
  			UpArray[0][dcindex] = getUplinkOfDC().get(dcindex + DCbase);
  			DownArray[0][dcindex] = getDownlinkOfDC().get(dcindex + DCbase);
  			String resourcelog = SlotArray[0][dcindex]+"\t"+(SlotArray[0][dcindex]/ori_idleTaskSlotsOfDC.get(dcindex+DCbase))+"\t"
  					+ UpArray[0][dcindex]+"\t"+(UpArray[0][dcindex]/ori_uplinkOfDC.get(dcindex+DCbase))+"\t"
  					+ DownArray[0][dcindex]+"\t"+(DownArray[0][dcindex]/ori_downlinkOfDC.get(dcindex+DCbase))+"\t";
  			log.info(resourcelog);
  		}
  		
//  		int remainingSlotNum = slotNum;
        int allNumOfJob = rankedList.size();
        int jobNumInOneLoop = allNumOfJob;
        // each job weight equal 1
        
        if(Parameters.isDebug) {
        	Log.printLine("original resource:");
            Log.printLine(DownArray[0][0]+" "+DownArray[0][1]+" "+DownArray[0][2]);
            Log.printLine(UpArray[0][0]+" "+UpArray[0][1]+" "+UpArray[0][2]);
        }
        
        
        int assignedTaskNumber = 0;
        int lastjobindex = 0;
        int srptJobNum = (int)Math.ceil(jobNumInOneLoop*Parameters.epsilon);
        // use break exit loop
    while(true) {
    	
        for (int jobindex = lastjobindex; jobindex < srptJobNum; jobindex++) {
        	Job job = (Job)rankedList.get(jobindex);
        	// submitTasks while update JobList info
			if(!taskOfJob.containsKey(job.getCloudletId())) {
				if(job.getTaskList().size() == 0) {
					job.jobId = job.getCloudletId();
    				taskOfJob.put(job.getCloudletId(), 1);
    				ackTaskOfJob.put(job.getCloudletId(), 0);
    				JobFactory.put(job.getCloudletId(), job);
    				usedSlotsOfJob.put(job.getCloudletId(), 0);
    			}else {
    				// when return remember to delete the item in the three tables
    				taskOfJob.put(job.getCloudletId(), job.getTaskList().size());
    				scheduledTaskOfJob.put(job.getCloudletId(), 0);
    				ackTaskOfJob.put(job.getCloudletId(), 0);
    				JobFactory.put(job.getCloudletId(), job);
    				usedSlotsOfJob.put(job.getCloudletId(), 0);
    				rescheduleTasks(job.getTaskList());
    			}
			}
        	int preAssignedSlots = (int)Math.round(slotNum/srptJobNum);
        	preAssignedSlots -= usedSlotsOfJob.get(job.getCloudletId());
        	int unscheduledTaskNum = job.unscheduledTaskList.size();
        	int greatAssignedTaskNum = unscheduledTaskNum - job.failedAssignTaskIndexInGreateAssign.size();
        	if(greatAssignedTaskNum == 0)
        		continue;
        	// whether preAssigned slots is enough for the original tasks in the job
        	double[] singlex = null;
        	Map<Integer, Double> bwOfSrcPos = null;
        	double totalBandwidth = 0d;
        	boolean allzeroflag = true;
        	int vnum = unscheduledTaskNum*Parameters.numberOfDC;
//        	if(greatAssignedTaskNum <= preAssignedSlots) {
        	if(preAssignedSlots > 0) {

        	
        	
        	// if no
	        	// solve the assignment based on the current resource
        		
        		if(Parameters.isGurobi == false) {
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
						cplex.addMinimize(cplex.scalProd(var, objvals));
						
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
										if(job.allRateMuArray[0][xindex] == 0) {
											itermOfTask[xindex] = cplex.prod(1e20d, var[xindex]);
										}else {
											itermOfTask[xindex] = cplex.prod(Parameters.delayAmongDCIndex[job.submitDCIndex][dcindex]
													+ job.workloadArray[xindex]/(job.allRateMuArray[0][xindex]
													- Parameters.r * job.allRateSigmaArray[0][xindex]), var[xindex]);
										}
										
									}else {
										itermOfTask[xindex] = cplex.prod(0.0, var[xindex]);
									}
								}
							}
							itermOfTask[vnumplusone-1] = cplex.prod(-1.0, var[vnumplusone-1]);
							rng[constraintIndex] = cplex.addLe(cplex.sum(itermOfTask), 0.0);
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
							rng[constraintIndex] = (Parameters.isConcernGeoNet == false)?
	    							cplex.addGe(cplex.sum(itermOfTask), 0.0d)
	    							:cplex.addLe(cplex.sum(itermOfTask), UpArray[0][datacenterindex]);
	    					
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
							rng[constraintIndex] = (Parameters.isConcernGeoNet == false)?
	    							cplex.addGe(cplex.sum(itermOfTask), 0.0d)
	    							:cplex.addLe(cplex.sum(itermOfTask), DownArray[0][datacenterindex]);
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
							if(Parameters.isDebug)
								Log.printLine("recompute greate resource for job"+job.getCloudletId());
							singlex = new double[vnum];
							double[] vresult = cplex.getValues(var);
							for(int vindex = 0; vindex < vnum; vindex++) {
								singlex[vindex] = vresult[vindex];
							}
							
		//					double[] slack = cplex.getSlacks(rng);
							System.out.println("Solution status = " + cplex.getStatus());
							
							//verify x
							
							for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
								if(preAssignedSlots <= 0) {
									for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
										int xindex = tindex * Parameters.numberOfDC + dcindex;
										singlex[xindex] = 0;
									}
								}
								boolean success = false;
								int datanumber = job.data[tindex];
								
								for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
									int xindex = tindex * Parameters.numberOfDC + dcindex;
									
									if(singlex[xindex] > 0 && success == false) {
										boolean inter_resourceEnough = true;
										// machines
										if((SlotArray[0][dcindex]-1)<0) {
											inter_resourceEnough = false;
										}
										
										totalBandwidth = 0d;
										// uplink
										bwOfSrcPos = new HashMap<>();
										if(Parameters.isConcernGeoNet == true) {
											
    										if(job.TotalTransferDataSize[xindex]>0) {
    											for(int dataindex = 0; dataindex < datanumber; dataindex++) {
    												double neededBw = job.bandwidth[xindex][dataindex];
    												totalBandwidth += neededBw;
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
    										// downlink
    										if(job.TotalTransferDataSize[xindex]>0 && inter_resourceEnough == true) {
    											if((DownArray[0][dcindex]-totalBandwidth)<0) {
    												inter_resourceEnough = false;
    											}
    										}
										}
										
										
										if(inter_resourceEnough == true) {
											success = true;
											singlex[xindex] = 1;
											preAssignedSlots -= 1;
											allzeroflag = false;
											// cut down resource
											// machines
											SlotArray[0][dcindex] -= 1;
											if(Parameters.isConcernGeoNet == true) {
												// downlink
    											if(job.TotalTransferDataSize[xindex]>0) {
    												DownArray[0][dcindex] -= totalBandwidth;
    											}
    											
    											// uplink
    											
    											if(job.TotalTransferDataSize[xindex]>0) {
    												for(int pos : bwOfSrcPos.keySet()) {
    													UpArray[0][pos] -= bwOfSrcPos.get(pos);
    												}
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
							if(Parameters.isDebug) {
								Log.printLine("updated resource-job"+job.getCloudletId()+":");
			                    Log.printLine(DownArray[0][0]+" "+DownArray[0][1]+" "+DownArray[0][2]);
			                    Log.printLine(UpArray[0][0]+" "+UpArray[0][1]+" "+UpArray[0][2]);
							}
		                    
						}else {
							cplex.end();
							// greedy assign for the tasks in the job as well as its copy
							// when there is some tasks do not be assigned then the copy is not needed
							// use the matlab jar
							if(Parameters.isDebug)
								Log.printLine("recompute with the greedy when cplex is unsolvable for job"+job.getCloudletId());
							singlex = new double[vnum];
							for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
								if(preAssignedSlots <= 0)
									break;
								Task task = job.unscheduledTaskList.get(tindex);
								int taskId = task.getCloudletId();
								int datanumber = task.numberOfData;
								boolean success = true;
								int successDC = -1;
								for(Map.Entry<Integer, Double> iterm:job.sortedListOfTask.get(taskId)) {
									int dcindex = iterm.getKey();
									int xindex = tindex * Parameters.numberOfDC + dcindex;
									success = true;
									if(job.uselessDCforTask[xindex] == 0) {
										success = false;
										break;
									}
									
									// when the dc is not too far
//    									if(job.uselessDCforTask[xindex] != 0) {
										// verify that the resource is enough
										
										// machines
										if((SlotArray[0][dcindex]-1)<0) {
											success = false;
											continue;
										}
										
										
										totalBandwidth = 0d;
										// uplink
										bwOfSrcPos = new HashMap<>();
										if(Parameters.isConcernGeoNet == true) {
											if(job.TotalTransferDataSize[xindex]>0) {
    											for(int dataindex = 0; dataindex < datanumber; dataindex++) {
    												double neededBw = job.bandwidth[xindex][dataindex];
    												totalBandwidth += neededBw;
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
    										
    										
    										// downlink
    										if(job.TotalTransferDataSize[xindex]>0 && success == true) {
    											if((DownArray[0][dcindex]-totalBandwidth)<0) {
    												success = false;
    												continue;
    											}
    										}
										}
										
										if(success == true) {
											SlotArray[0][dcindex] -= 1;
											
											if(Parameters.isConcernGeoNet == true) {
												if(job.TotalTransferDataSize[xindex]>0) {
    												DownArray[0][dcindex] -= totalBandwidth;

    											}
    											for(int pos : bwOfSrcPos.keySet()) {
    												UpArray[0][pos]-=bwOfSrcPos.get(pos);
    											}
											}
											
											successDC = dcindex;
											break;
										}
//    									}
								}
								if(success == true && successDC != -1) {
									
									// store the greatest assignment info in the job with the current resource
									int xindex = tindex * Parameters.numberOfDC + successDC;
									allzeroflag = false;
									singlex[xindex] = 1;
									preAssignedSlots -= 1;
								}
							}
							if(Parameters.isDebug) {
								Log.printLine("updated resource-job"+job.getCloudletId()+":");
			                    Log.printLine(DownArray[0][0]+" "+DownArray[0][1]+" "+DownArray[0][2]);
			                    Log.printLine(UpArray[0][0]+" "+UpArray[0][1]+" "+UpArray[0][2]);
			            		
							}
		                    
							
						}	
					} catch (IloException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
        		}else {
        			try {
        				int vnumplusone = 1 + unscheduledTaskNum*Parameters.numberOfDC;
						GRBEnv env = new GRBEnv();
						GRBModel model = new GRBModel(env);
						GRBVar[] vars = new GRBVar[vnumplusone];
						GRBLinExpr expr = new GRBLinExpr();
						for(int vindex = 0; vindex < vnumplusone; vindex++) {
							if(vindex == (vnumplusone - 1)) {
								vars[vindex] = model.addVar(0.0d, Double.MAX_VALUE, 1.0d, GRB.CONTINUOUS, "x"+String.valueOf(vindex));
								expr.addTerm(1.0d, vars[vindex]);
							}else {
								vars[vindex] = model.addVar(0.0d, 1.0d, 0.0d, GRB.BINARY, "x"+String.valueOf(vindex));
								expr.addTerm(0.0d, vars[vindex]);
							}
						}
						model.setObjective(expr, GRB.MINIMIZE);
						
						int constraintsNum = 2 * unscheduledTaskNum + 3 * Parameters.numberOfDC + job.uselessConstraintsNum;
	    				int constraintIndex = 0;
	    				// constraints
	    				// extra constraints about task
	    				
	    				for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
	    					expr = new GRBLinExpr();
	    					for(int taskindex = 0; taskindex < unscheduledTaskNum; taskindex++) {
	    						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
	    							int xindex = taskindex*Parameters.numberOfDC + dcindex;
	    							if(taskindex == tindex) {
	    								if(job.allRateMuArray[0][xindex] == 0) {
	    									expr.addTerm(1e20d, vars[xindex]);
	    								}else {
	    									expr.addTerm(Parameters.delayAmongDCIndex[job.submitDCIndex][dcindex]
	    											+ job.workloadArray[xindex]/(job.allRateMuArray[0][xindex]
		    										- Parameters.r * job.allRateSigmaArray[0][xindex]), vars[xindex]);
	    								}
	    								
	    							}else {
	    								expr.addTerm(0.0d, vars[xindex]);
	    							}
	    							
	    						}
	    					}
	    					expr.addTerm(-1.0d, vars[vnum]);
	    					model.addConstr(expr, GRB.LESS_EQUAL, 0.0d, "c"+String.valueOf(constraintIndex));
	    					constraintIndex++;
	    				}
	    				
	    				// each task has one execution among DCs
	    				for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
	    					expr = new GRBLinExpr();
	    					for(int taskindex = 0; taskindex < unscheduledTaskNum; taskindex++) {
	    						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
	    							int xindex = taskindex*Parameters.numberOfDC + dcindex;
	    							if(taskindex == tindex) {
	    								expr.addTerm(1.0d, vars[xindex]);
	    							}else {
	    								expr.addTerm(0.0d, vars[xindex]);
	    							}
	    						}
	    					}
	    					expr.addTerm(0.0d, vars[vnum]);
	    					model.addConstr(expr, GRB.EQUAL, 1.0, "c"+String.valueOf(constraintIndex));
	    					constraintIndex++;
	    				}
	    				
	    				// machine limitation
	    				for(int datacenterindex = 0; datacenterindex < Parameters.numberOfDC; datacenterindex++) {
	    					expr = new GRBLinExpr();
	    					for(int taskindex = 0; taskindex < unscheduledTaskNum; taskindex++) {
	    						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
	    							int xindex = taskindex*Parameters.numberOfDC + dcindex;
	    							if(dcindex == datacenterindex) {
	    								expr.addTerm(1.0, vars[xindex]);
	    							}else {
	    								expr.addTerm(0.0, vars[xindex]);
	    							}
	    						}
	    					}
	    					expr.addTerm(0.0d, vars[vnum]);
	    					model.addConstr(expr, GRB.LESS_EQUAL, SlotArray[0][datacenterindex], "c"+String.valueOf(constraintIndex));
	    					constraintIndex++;
	    				}
	    				
	    				// uplink bandwidth limitation
	    				for(int datacenterindex = 0; datacenterindex < Parameters.numberOfDC; datacenterindex++) {
	    					expr = new GRBLinExpr();
	    					for(int taskindex = 0; taskindex < unscheduledTaskNum; taskindex++) {
	    						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
	    							int xindex = taskindex*Parameters.numberOfDC + dcindex;
	    							double upsum = 0d;
	    							for(int dataindex = 0; dataindex < job.data[taskindex]; dataindex++) {
	    								if(job.datapos[taskindex][dataindex] == datacenterindex) {
	    									upsum += job.bandwidth[xindex][dataindex];
	    								}
	    							}
	    							expr.addTerm(upsum, vars[xindex]);
	    						}
	    					}
	    					expr.addTerm(0.0d, vars[vnum]);
	    					if(Parameters.isConcernGeoNet == false) {
								model.addConstr(expr, GRB.GREATER_EQUAL, 0.0d, "c"+String.valueOf(constraintIndex));
	    					}else {
								model.addConstr(expr, GRB.LESS_EQUAL, UpArray[0][datacenterindex], "c"+String.valueOf(constraintIndex));

	    					}
	    					constraintIndex++;
	    				}
	    				
	    				// downlink bandwidth limitation
	    				
	    				for(int datacenterindex = 0; datacenterindex < Parameters.numberOfDC; datacenterindex++) {
	    					expr = new GRBLinExpr();
	    					for(int taskindex = 0; taskindex < unscheduledTaskNum; taskindex++) {
	    						for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
	    							int xindex = taskindex*Parameters.numberOfDC + dcindex;
	    							if(dcindex == datacenterindex) {
	    								double downsum = 0d;
	    								for(int dataindex = 0; dataindex < job.data[taskindex]; dataindex++) {
	    										downsum += job.bandwidth[xindex][dataindex];
	    								}
	    								expr.addTerm(downsum, vars[xindex]);
	    							}else {
	    								expr.addTerm(0.0d, vars[xindex]);
	    							}
	    						}
	    					}
	    					expr.addTerm(0.0d, vars[vnum]);
	    					if(Parameters.isConcernGeoNet == false) {
	        					model.addConstr(expr, GRB.GREATER_EQUAL, 0.0d, "c"+String.valueOf(constraintIndex));
	    					}else {
	        					model.addConstr(expr, GRB.LESS_EQUAL, DownArray[0][datacenterindex], "c"+String.valueOf(constraintIndex));
	    					}
	    					constraintIndex++;
	    				}
	    				
	    				// uselessDC limitation
	    				for(int xindex = 0; xindex < vnum; xindex++) {
	    					expr = new GRBLinExpr();
	    					if(job.uselessDCforTask[xindex] == 0) {
	    						expr.addTerm(1.0d, vars[xindex]);
	    						model.addConstr(expr, GRB.EQUAL, 0.0d, "c"+String.valueOf(constraintIndex));
	    						constraintIndex++;
	    					}
	    				}
	    				
	    				model.optimize();
	    				int status = model.get(GRB.IntAttr.Status);
	    				if(status == GRB.Status.OPTIMAL) {
	    					singlex = model.get(GRB.DoubleAttr.X, model.getVars());
	    					//verify x
							
							for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
								if(preAssignedSlots <= 0) {
									for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
										int xindex = tindex * Parameters.numberOfDC + dcindex;
										singlex[xindex] = 0;
									}
								}
								boolean success = false;
								int datanumber = job.data[tindex];
								
								for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
									int xindex = tindex * Parameters.numberOfDC + dcindex;
									
									if(singlex[xindex] > 0 && success == false) {
										boolean inter_resourceEnough = true;
										// machines
										if((SlotArray[0][dcindex]-1)<0) {
											inter_resourceEnough = false;
										}
										
										
										totalBandwidth = 0d;
										// uplink
										bwOfSrcPos = new HashMap<>();
										if(Parameters.isConcernGeoNet == true) {
											if(job.TotalTransferDataSize[xindex]>0) {
    											for(int dataindex = 0; dataindex < datanumber; dataindex++) {
    												double neededBw = job.bandwidth[xindex][dataindex];
    												totalBandwidth += neededBw;
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
    										// downlink
    										if(job.TotalTransferDataSize[xindex]>0 && inter_resourceEnough == true) {
    											if((DownArray[0][dcindex]-totalBandwidth)<0) {
    												inter_resourceEnough = false;
    											}
    										}
										}
										
										
										if(inter_resourceEnough == true) {
											success = true;
											singlex[xindex] = 1;
											preAssignedSlots -= 1;
											allzeroflag = false;
											// cut down resource
											// machines
											SlotArray[0][dcindex] -= 1;
											
											if(Parameters.isConcernGeoNet == true) {
												// downlink
    											if(job.TotalTransferDataSize[xindex]>0) {
    												DownArray[0][dcindex] -= totalBandwidth;
    											}
    											
    											// uplink
    											
    											if(job.TotalTransferDataSize[xindex]>0) {
    												for(int pos : bwOfSrcPos.keySet()) {
    													UpArray[0][pos] -= bwOfSrcPos.get(pos);
    												}
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

	    				}else {
	    					//greedy
	    					singlex = new double[vnum];

							for(int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
								if(preAssignedSlots <= 0)
									break;
								Task task = job.unscheduledTaskList.get(tindex);
								int taskId = task.getCloudletId();
								int datanumber = task.numberOfData;
								boolean success = true;
								int successDC = -1;
								for(Map.Entry<Integer, Double> iterm:job.sortedListOfTask.get(taskId)) {
									int dcindex = iterm.getKey();
									int xindex = tindex * Parameters.numberOfDC + dcindex;
									success = true;
									if(job.uselessDCforTask[xindex] == 0) {
										success = false;
										break;
									}
									
									// when the dc is not too far
//    									if(job.uselessDCforTask[xindex] != 0) {
										// verify that the resource is enough
										
										// machines
										if((SlotArray[0][dcindex]-1)<0) {
											success = false;
											continue;
										}
										
										
										totalBandwidth = 0d;
										// uplink
										bwOfSrcPos = new HashMap<>();

										if(Parameters.isConcernGeoNet == true) {
											if(job.TotalTransferDataSize[xindex]>0) {
    											for(int dataindex = 0; dataindex < datanumber; dataindex++) {
    												double neededBw = job.bandwidth[xindex][dataindex];
    												totalBandwidth += neededBw;
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
    										
    										
    										// downlink
    										if(job.TotalTransferDataSize[xindex]>0 && success == true) {
    											if((DownArray[0][dcindex]-totalBandwidth)<0) {
    												success = false;
    												continue;
    											}
    										}
										}
										
										if(success == true) {
											SlotArray[0][dcindex] -= 1;
											
											if(Parameters.isConcernGeoNet == true) {
												if(job.TotalTransferDataSize[xindex]>0) {
    												DownArray[0][dcindex] -= totalBandwidth;

    											}
    											for(int pos : bwOfSrcPos.keySet()) {
    												UpArray[0][pos]-=bwOfSrcPos.get(pos);
    											}
											}
											
											successDC = dcindex;
											break;
										}
//    									}
								}
								if(success == true && successDC != -1) {
									
									// store the greatest assignment info in the job with the current resource
									int xindex = tindex * Parameters.numberOfDC + successDC;
									allzeroflag = false;
									singlex[xindex] = 1;
									preAssignedSlots -= 1;
								}
							}

	    				}
	    				
	    				model.dispose();
	    				env.dispose();
					} catch (GRBException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
        		}
        		
        		if(allzeroflag == false && (Parameters.copystrategy == 5 || Parameters.copystrategy == 0 || Parameters.copystrategy == 6 || preAssignedSlots <= 0)) {
        			
        			if(Parameters.copystrategy == 6 && preAssignedSlots > 0) {
        				//assign task copy in singlex
            			double taskNumOfJob = job.getTaskList().size();
            			for (int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
            				
            				int originalDCindex = -1;
            				double taskPro = 0d;
            				for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
            					if(singlex[tindex*Parameters.numberOfDC + dcindex]!=0) {
            						originalDCindex = dcindex;
            						taskPro = Parameters.likelihoodOfFailure[dcindex];
            						break;
            					}
            				}
            				if(originalDCindex != -1) {
            					Task task = job.unscheduledTaskList.get(tindex);
                				
                				int numberOfCopy = (int) (Math.log(1
                						-Math.pow(1-Parameters.acceptProOfJob, 1/taskNumOfJob))/Math.log(taskPro)+0.5);
                				int numberOfExeCopy = Math.max(1, numberOfCopy);
                				double judgePro = Math.pow(taskPro, numberOfExeCopy);
                				boolean success = true;
                				int successDC = -1;
                				for(int copyindex = 0; copyindex < (numberOfExeCopy-1); copyindex++) {
                					// when resource is not enough
                					// when the last copy do not have the appropriate position
                					// when the requirement is satisfied
                					if(preAssignedSlots <= 0 || success == false || taskPro <= judgePro)
                						break;
                					// greedy choose the vm
                    				int taskId = task.getCloudletId();
                    				int datanumber = task.numberOfData;
                    				success = true;
                    				successDC = -1;
                    				for(int listindex = 0; listindex < Parameters.numberOfDC; listindex++) {
                    					int dcindex = Parameters.sortedlikelihoodOfFailure[listindex];
                    					int xindex = tindex * Parameters.numberOfDC + dcindex;
                    					if(task.uselessDC[dcindex] == 0) {
                    						//success = false;
                    						continue;
                    					}
                    					
//                    					if(taskPro <= judgePro) {
//                    						success = false;
//                    						break;
//                    					}
                    					
                    					success = true;
                    					// when the dc is not too far
//                    					if(job.uselessDCforTask[xindex] != 0) {
                    						// verify that the resource is enough
                    						
                    						// machines
                    						if((SlotArray[0][dcindex]-1)<0) {
                    							success = false;
                    							continue;
                    						}
                    						
                    						
                    						totalBandwidth = 0d;
                    						// uplink
                    						bwOfSrcPos = new HashMap<>();
                    						if(Parameters.isConcernGeoNet == true) {
                    							if(task.TotalTransferDataSize[dcindex]>0) {
                    								for(int dataindex = 0; dataindex < datanumber; dataindex++) {
                    									double neededBw = task.bandwidth[dcindex][dataindex];
                    									totalBandwidth += totalBandwidth;
                    									int srcPos = (int) task.positionOfData[dataindex];
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
                    							
                    							// downlink
                    							if(task.TotalTransferDataSize[dcindex]>0 && success == true) {
                    								if((DownArray[0][dcindex]-totalBandwidth)<0) {
                    									success = false;
                    									continue;
                    								}
                    							}
                    						}
                    						
                    						
                    						
                    						if(success == true) {
                    							SlotArray[0][dcindex] -= 1;
                    							
                    							if(Parameters.isConcernGeoNet == true) {
                    								if(task.TotalTransferDataSize[dcindex]>0) {
                    									DownArray[0][dcindex] -= totalBandwidth;

                    								}
                    								for(int pos : bwOfSrcPos.keySet()) {
                    									UpArray[0][pos]-=bwOfSrcPos.get(pos);
                    								}
                    							}
                    							singlex[xindex] += 1;
                    							preAssignedSlots -= 1;
                    							successDC = dcindex;
                    							taskPro *= Parameters.likelihoodOfFailure[dcindex];
                    							break;
                    						}
//                    					}
                    				}
                    				
                				}
                				
                			}
            			}
            			
        			}
        			
        			
        			
        			
        			// Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
					Queue<Task> taskSubmitted = new LinkedList<>();
					// successful assignment
					
					for (int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
						Task task = job.unscheduledTaskList.get(tindex);
						boolean submitflag = false;
						for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
							if (singlex[tindex*Parameters.numberOfDC + dcindex] != 0) {
								if(submitflag == false) {
									submitflag = true;
								}
								int submittedNum = (int) singlex[tindex*Parameters.numberOfDC + dcindex];
								for(int copyindex = 0; copyindex < submittedNum; copyindex++) {
									Vm vm = idleTaskSlotsOfDC.get(dcindex + DCbase).remove();
									if (tasks.containsKey(task.getCloudletId())) {
										Task speculativeTask = new Task(task);
										cloneTask(speculativeTask,task);
										speculativeTask.setAssignmentDCId(dcindex + DCbase);
										speculativeTask.assignmentDCindex = dcindex;
										speculativeTask.setSpeculativeCopy(true);
										speculativeTask.incBw((long)(task.TotalTransferDataSize[dcindex]/1024d));
										speculativeTasks.get(speculativeTask.getCloudletId()).add(speculativeTask);
										submitSpeculativeTask(speculativeTask, vm);
									} else {
										task.setAssignmentDCId(dcindex + DCbase);
										task.assignmentDCindex = dcindex;
										task.incBw((long)(task.TotalTransferDataSize[dcindex]/1024d));
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
					continue;
        		}
        		
        		// judge whether x is all zero
        		double[] x = null;
        		if(allzeroflag == false) {
        			// copy based on the fresh assignment
            		MWNumericArray xOrig = null;
            		MWNumericArray tasknum = null;
            		MWNumericArray dcnum = null;
            		MWNumericArray submittedIndex = null;
            		MWNumericArray allRateMuArray = null;
            		MWNumericArray allRateSigmaArray = null;
            		MWNumericArray workloadArray = null;
            		MWNumericArray TotalTransferDataSize = null;
            		MWNumericArray data = null;
            		MWNumericArray datapos = null;
            		MWNumericArray bandwidth = null;
            		MWNumericArray Slot = null;
            		MWNumericArray Up = null;
            		MWNumericArray Down = null;
            		MWNumericArray DCFailPro = null;
            		MWNumericArray FailThreshold = null;
            		MWNumericArray uselessDCforTask = null;
            		MWNumericArray r = null;
            		MWNumericArray slotlimit = null;
            		MWNumericArray isGeoNet = null;
            		MWNumericArray isDCFail = null;
            		MWNumericArray delayAmongDC = null;
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
						isGeoNet = MWNumericArray.newInstance(dims, MWClassID.INT16,MWComplexity.REAL);
						isDCFail = MWNumericArray.newInstance(dims, MWClassID.INT16,MWComplexity.REAL);
						FailThreshold = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						submittedIndex = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
						dims[1] = unscheduledTaskNum;
						data = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
						dims[1] = Parameters.numberOfDC;
						Slot = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						Up = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						Down = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						DCFailPro = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						dims[0] = Parameters.numberOfDC;
						delayAmongDC = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						dims[0] = 1;
						dims[1] = vnum;
						xOrig = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						allRateMuArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						allRateSigmaArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
						workloadArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE,MWComplexity.REAL);
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
						if(Parameters.isConcernGeoNet == true)
							isGeoNet.set(1, 1);
						else
							isGeoNet.set(1, 0);
						
						if(Parameters.isConcernDCFail == true)
							isDCFail.set(1, 1);
						else
							isDCFail.set(1, 0);
						
						FailThreshold.set(1, Parameters.DCFailThreshold);
						submittedIndex.set(1, job.submitDCIndex);
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
							// xOrig allRateMu allRateSigma workload uselessDCforTask bandwidth
							for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
								int xindex = tindex * Parameters.numberOfDC + dcindex;
								pos[0] = 1;
								pos[1] = xindex + 1;
								xOrig.set(pos, singlex[xindex]);
								allRateMuArray.set(pos, job.allRateMuArray[0][xindex]);
								allRateSigmaArray.set(pos, job.allRateSigmaArray[0][xindex]);
								workloadArray.set(pos, job.workloadArray[xindex]);
								uselessDCforTask.set(pos, job.uselessDCforTask[xindex]);
								TotalTransferDataSize.set(pos, job.TotalTransferDataSize[xindex]);
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
							DCFailPro.set(pos, Parameters.likelihoodOfDCFailure[dcindex]);
							for(int dcindex_in = 0; dcindex_in < Parameters.numberOfDC; dcindex_in++) {
								pos[0] = dcindex + 1;
								pos[1] = dcindex_in + 1;
								if(Parameters.delayAmongDCIndex[dcindex][dcindex_in] < 1e20d)
									delayAmongDC.set(pos, Parameters.delayAmongDCIndex[dcindex][dcindex_in]);
								else
									delayAmongDC.set(pos,0d);
							}
						}
						
						switch(Parameters.copystrategy) {
						case 1:
							result = taskAssign.copyStrategy(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,workloadArray
									,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit
									,isGeoNet,isDCFail,DCFailPro,FailThreshold,delayAmongDC,submittedIndex);
							break;
						case 2:
							result = taskAssign.copyStrategy_optimizeExp_Traverse(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,workloadArray
									,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit
									,isGeoNet,isDCFail,DCFailPro,FailThreshold,delayAmongDC,submittedIndex);
							break;
						case 3:
							result = taskAssign.copyStrategy_optimizeAll_orderedRes(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,workloadArray
									,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit
									,isGeoNet,isDCFail,DCFailPro,FailThreshold,delayAmongDC,submittedIndex);
							break;
						case 4:
							result = taskAssign.copyStrategy_optimizeAll_Traverse(4,xOrig,tasknum,dcnum,allRateMuArray,allRateSigmaArray,workloadArray
									,TotalTransferDataSize,data,datapos,bandwidth,Slot,Up,Down,uselessDCforTask,r,slotlimit
									,isGeoNet,isDCFail,DCFailPro,FailThreshold,delayAmongDC,submittedIndex);
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
						if(Parameters.isDebug) {
							Log.printLine("updated resource-copy-job"+job.getCloudletId()+":");
    	                    Log.printLine(DownArray[0][0]+" "+DownArray[0][1]+" "+DownArray[0][2]);
    	                    Log.printLine(UpArray[0][0]+" "+UpArray[0][1]+" "+UpArray[0][2]);
    	            		
						}
	                    
						// Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
						Queue<Task> taskSubmitted = new LinkedList<>();
						// successful assignment
						
						for (int tindex = 0; tindex < unscheduledTaskNum; tindex++) {
							Task task = job.unscheduledTaskList.get(tindex);
							boolean submitflag = false;
							for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
								if (x[tindex*Parameters.numberOfDC + dcindex] > 0) {
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
											speculativeTask.incBw((long)(task.TotalTransferDataSize[dcindex]/1024d));
											speculativeTasks.get(speculativeTask.getCloudletId()).add(speculativeTask);
											assignedTaskNumber++;
											submitSpeculativeTask(speculativeTask, vm);
										} else {
											task.setAssignmentDCId(dcindex + DCbase);
											task.assignmentDCindex = dcindex;
											task.incBw((long)(task.TotalTransferDataSize[dcindex]/1024d));
											tasks.put(task.getCloudletId(), task);
											assignedTaskNumber++;
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
						MWNumericArray.disposeArray(isGeoNet);
						MWNumericArray.disposeArray(isDCFail);
						MWNumericArray.disposeArray(submittedIndex);
						MWNumericArray.disposeArray(allRateMuArray);
						MWNumericArray.disposeArray(allRateSigmaArray);
						MWNumericArray.disposeArray(TotalTransferDataSize);
						MWNumericArray.disposeArray(workloadArray);
						MWNumericArray.disposeArray(data);
						MWNumericArray.disposeArray(datapos);
						MWNumericArray.disposeArray(bandwidth);
						MWNumericArray.disposeArray(Slot);
						MWNumericArray.disposeArray(Up);
						MWNumericArray.disposeArray(Down);
						MWNumericArray.disposeArray(delayAmongDC);
						MWNumericArray.disposeArray(DCFailPro);
						MWNumericArray.disposeArray(uselessDCforTask);
						MWNumericArray.disposeArray(r);
						MWNumericArray.disposeArray(slotlimit);
						MWNumericArray.disposeArray(FailThreshold);
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
        
        if(allNumOfJob <= srptJobNum) {
        	break;
        }
        
        int remainingSlots = 0;
        for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
        	remainingSlots += SlotArray[0][dcindex];
        }
        if(remainingSlots == 0) {
        	break;
        }
        
        lastjobindex = srptJobNum;
        jobNumInOneLoop = allNumOfJob - srptJobNum;
        srptJobNum = Math.min(allNumOfJob, srptJobNum + (int)Math.ceil(jobNumInOneLoop*Parameters.epsilon));
        	
    }
        
        
	    if(Parameters.copystrategy == 5 || Parameters.copystrategy == 1) {
	    	// assign speculative for the running tasks
	    	LateStrategy(SlotArray[0],UpArray[0],DownArray[0]);
	    }
        getCloudletList().removeAll(scheduler.getScheduledList());
        for(int sindex = 0; sindex < scheduler.getScheduledList().size(); sindex++) {
        	Job job = (Job)scheduler.getScheduledList().get(sindex);
        	if(!getCloudletSubmittedList().contains(job)) {
        		getCloudletSubmittedList().add(job);
        		cloudletsSubmitted += 1;
        	}
        	
        }
        lastProcessTime = CloudSim.clock();
        log.info("Resource");
        log.info(CloudSim.clock+"\t");
        for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
        	String resourcelog = SlotArray[0][dcindex]+"\t"+(SlotArray[0][dcindex]/ori_idleTaskSlotsOfDC.get(dcindex+DCbase))+"\t"
					+ UpArray[0][dcindex]+"\t"+(UpArray[0][dcindex]/ori_uplinkOfDC.get(dcindex+DCbase))+"\t"
					+ DownArray[0][dcindex]+"\t"+(DownArray[0][dcindex]/ori_downlinkOfDC.get(dcindex+DCbase))+"\t";
			log.info(resourcelog);
        }
        if(assignedTaskNumber == 0) {
        	int a = 1;
        	a = a + 1;
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
    
    
    public void rescheduleTasks(List<Task> taskList) {
		// TODO Auto-generated method stub
    	for (Task task : taskList) {
			progressScores.put(task, 0d);
		}
	}

    private void computeProgressScore(Task task) {
		double actualProgressScore = (double) (task.getCloudletFinishedSoFar())
				/ (double) (task.getCloudletLength());
		// the distortion is higher if task is really close to finish or just
		// started recently
		double distortionIntensity = 1d - Math
				.abs(1d - actualProgressScore * 2d);
		double distortion = Parameters.numGen.nextGaussian()
				* Parameters.distortionCV * distortionIntensity;
		double perceivedProgressScore = actualProgressScore + distortion;
		double time = CloudSim.clock() - task.getExecStartTime();
		int taskId = task.getCloudletId();
		if(speculativeTasks.get(taskId).size()>0) {
			LinkedList<Task> speculativeList = (LinkedList<Task>)speculativeTasks.get(taskId);
			Iterator<Task> it = speculativeList.iterator();
			while(it.hasNext()) {
				Task speculativeTask = it.next();
				double actualProgressScore_s = (double) (speculativeTask.getCloudletFinishedSoFar())
						/ (double) (speculativeTask.getCloudletLength());
				// the distortion is higher if task is really close to finish or just
				// started recently
				double distortionIntensity_s = 1d - Math
						.abs(1d - actualProgressScore_s * 2d);
				double distortion_s = Parameters.numGen.nextGaussian()
						* Parameters.distortionCV * distortionIntensity_s;
				double perceivedProgressScore_s = actualProgressScore_s + distortion_s;
				if(perceivedProgressScore_s > perceivedProgressScore) {
					perceivedProgressScore = perceivedProgressScore_s;
					time = CloudSim.clock() - speculativeTask.getExecStartTime();
				}
				
			}
			
		}
		progressScores.put(task,
				(perceivedProgressScore > 1) ? 0.99
						: ((perceivedProgressScore < 0) ? 0.01
								: perceivedProgressScore));
		timesTaskHasBeenRunning.put(task,time);
	}

	private void computeProgressRate(Task task) {
		computeProgressScore(task);
		progressRates.put(
				task,
				(timesTaskHasBeenRunning.get(task) == 0) ? Double.MAX_VALUE
						: progressScores.get(task)
								/ timesTaskHasBeenRunning.get(task));
	}

	public void computeEstimatedTimeToCompletion(Task task) {
		computeProgressRate(task);
		estimatedTimesToCompletion.put(
				task,
				(progressRates.get(task) == 0) ? Double.MAX_VALUE
						: (1d - progressScores.get(task))
								/ progressRates.get(task));
	}
	
	

	public void LateStrategy(double[] SlotArray,double[] UpArray,double[] DownArray) {
		// TODO Auto-generated method stub
    	// compute candidates
		// compute the sum of progress scores for all vms
		// Map<Integer, Double> vmIdToSumOfProgressScores = new HashMap<>();
//		for (Vm runningVm : nSucceededTasksPerVm.keySet()) {
//			vmIdToSumOfProgressScores.put(runningVm.getId(),
//					(double) nSucceededTasksPerVm.get(runningVm));
//		}

		for (Integer key : tasks.keySet()) {
			Task t = tasks.get(key);
			computeEstimatedTimeToCompletion(t);
//			vmIdToSumOfProgressScores.put(
//					t.getVmId(),
//					vmIdToSumOfProgressScores.get(t.getVmId())
//							+ progressScores.get(t));
		}

//		// compute the quantiles of task and node slowness
//		List<Vm> runningVms = new ArrayList<>(nSucceededTasksPerVm.keySet());
//		Collections.sort(runningVms, new VmSumOfProgressScoresComparator(
//				vmIdToSumOfProgressScores));
//		int quantileIndex = (int) (runningVms.size() * slowNodeThreshold - 0.5);
//		currentSlowNodeThreshold = vmIdToSumOfProgressScores.get(runningVms
//				.get(quantileIndex).getId());

		List<Task> runningTasks = new ArrayList<>(tasks.values());
		Collections.sort(runningTasks, new TaskProgressRateComparator());
		int quantileIndex = (int) (runningTasks.size() * slowTaskThreshold - 0.5);
		currentSlowTaskThreshold = (runningTasks.size() > 0) ? progressRates
				.get(runningTasks.get(quantileIndex)) : -1;

		// determine a candidate for speculative execution
		List<Task> candidates = new LinkedList<>();
		for (Integer key : tasks.keySet()) {
			Task candidate = tasks.get(key);
			if (progressRates.get(candidate) < currentSlowTaskThreshold) {
				System.out.println(progressRates.get(candidate) + " "
						+ currentSlowTaskThreshold);
				candidates.add(candidate);
			}
		}
		Collections.sort(candidates,
				new TaskEstimatedTimeToCompletionComparator());
		
		
  		
		for(int cindex = 0; cindex < candidates.size(); cindex++) {
			if(sizeOfSpeculative() >= speculativeCapAbs) {
				break;
			}
			
			Task task = candidates.get((candidates.size() - 1 - cindex));
			int taskId = task.getCloudletId();
			int datanumber = task.numberOfData;
			boolean success = true;
			int successDC = -1;
			
			switch (Parameters.OriginalVmChooseStrategy) {
			case 0:
				// stay in original datacenter
				successDC = task.assignmentDCindex;
				if((SlotArray[successDC]-1)<0) {
					success = false;
				}else {
					SlotArray[successDC] -= 1;
				}
				break;
			case 2:
				// greedy choose the vm
				
				for(int listindex = 0; listindex < Parameters.numberOfDC; listindex++) {
					int dcindex = task.orderedDClist[listindex];
					if(task.uselessDC[dcindex] == 0) {
						success = false;
						continue;
					}
					
					success = true;
					// when the dc is not too far
//					if(job.uselessDCforTask[xindex] != 0) {
						// verify that the resource is enough
						
						// machines
						if((SlotArray[dcindex]-1)<0) {
							success = false;
							continue;
						}
						
						
						double totalBandwidth = 0d;
						// uplink
						Map<Integer, Double> bwOfSrcPos = new HashMap<>();
						if(Parameters.isConcernGeoNet == true) {
							if(task.TotalTransferDataSize[dcindex]>0) {
								for(int dataindex = 0; dataindex < datanumber; dataindex++) {
									double neededBw = task.bandwidth[dcindex][dataindex];
									totalBandwidth += totalBandwidth;
									int srcPos = (int) task.positionOfData[dataindex];
									if(bwOfSrcPos.containsKey(srcPos)) {
										double oldvalue = bwOfSrcPos.get(srcPos);
										bwOfSrcPos.put(srcPos, oldvalue + neededBw);
									}else {
										bwOfSrcPos.put(srcPos, 0 + neededBw);
									}
								}
								for(int pos : bwOfSrcPos.keySet()) {
									if((UpArray[pos]-bwOfSrcPos.get(pos))<0) {
										success = false;
										break;
									}
								}
							}
							
							// downlink
							if(task.TotalTransferDataSize[dcindex]>0 && success == true) {
								if((DownArray[dcindex]-totalBandwidth)<0) {
									success = false;
									continue;
								}
							}
						}
						
						
						
						if(success == true) {
							SlotArray[dcindex] -= 1;
							
							if(Parameters.isConcernGeoNet == true) {
								if(task.TotalTransferDataSize[dcindex]>0) {
									DownArray[dcindex] -= totalBandwidth;

								}
								for(int pos : bwOfSrcPos.keySet()) {
									UpArray[pos]-=bwOfSrcPos.get(pos);
								}
							}
							
							successDC = dcindex;
							break;
						}
//					}
				}
				break;
			default:
				break;
			}
			
			
			
			
			// randome choose the vm
			
			
			
			
			if(success == true && successDC != -1) {
				
				
				// resource aware Mantri
				
				
				
				// assign the speculative
				Vm vm = idleTaskSlotsOfDC.get(successDC + DCbase).remove();
				if (tasks.containsKey(task.getCloudletId())) {
					Task speculativeTask = new Task(task);
					cloneTask(speculativeTask,task);
					speculativeTask.setAssignmentDCId(successDC + DCbase);
					speculativeTask.assignmentDCindex = successDC;
					speculativeTask.setSpeculativeCopy(true);
					speculativeTask.incBw((long)(task.TotalTransferDataSize[successDC]/1024d));
					speculativeTasks.get(speculativeTask.getCloudletId()).add(speculativeTask);
					submitSpeculativeTask(speculativeTask, vm);
				}
				
			}
			
		}
		

	}

	public int sizeOfSpeculative() {
		// TODO Auto-generated method stub
		int size = 0;
		for(Integer key:speculativeTasks.keySet()) {
			size += speculativeTasks.get(key).size();
		}
		return size;
	}

	private void cloneTask(Task speculativeTask, Task task) {
		speculativeTask.setDepth(task.getDepth());
		speculativeTask.setImpact(task.getImpact());
		speculativeTask.numberOfData = task.numberOfData;
		speculativeTask.jobId = task.jobId;
		speculativeTask.setSpeculativeCopy(true);
		speculativeTask.arrivalTime = task.arrivalTime;
//		speculativeTask.setExecStartTime(task.getExecStartTime());
		speculativeTask.earliestStartTime = task.earliestStartTime;
		speculativeTask.oriCloudletLength = task.oriCloudletLength;
		// initial
		speculativeTask.positionOfData = new int[speculativeTask.numberOfData];
		speculativeTask.sizeOfData = new int[speculativeTask.numberOfData];
		speculativeTask.requiredBandwidth = new double[speculativeTask.numberOfData];
		speculativeTask.positionOfDataID = new int[speculativeTask.numberOfData];
		for(int dataindex = 0; dataindex < speculativeTask.numberOfData; dataindex++) {
			speculativeTask.positionOfData[dataindex] = task.positionOfData[dataindex];
			speculativeTask.sizeOfData[dataindex] = task.sizeOfData[dataindex];
//			speculativeTask.requiredBandwidth[dataindex] = task.requiredBandwidth[dataindex];
			speculativeTask.positionOfDataID[dataindex] = task.positionOfDataID[dataindex];
		}
		speculativeTask.numberOfTransferData = new int[Parameters.numberOfDC];
		speculativeTask.TotalTransferDataSize = new double[Parameters.numberOfDC];
		speculativeTask.transferDataSize = new double[Parameters.numberOfDC][Parameters.ubOfData];
		speculativeTask.bandwidth = new double[Parameters.numberOfDC][Parameters.ubOfData];
		speculativeTask.orderedDClist = new int[Parameters.numberOfDC];
		speculativeTask.rateExpectation = new double[Parameters.numberOfDC];
		speculativeTask.uselessDC = new int[Parameters.numberOfDC];
		for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
			speculativeTask.numberOfTransferData[dcindex] = task.numberOfTransferData[dcindex];
			speculativeTask.TotalTransferDataSize[dcindex] = task.TotalTransferDataSize[dcindex];
			speculativeTask.orderedDClist[dcindex] = task.orderedDClist[dcindex];
			speculativeTask.rateExpectation[dcindex] = task.rateExpectation[dcindex];
			speculativeTask.uselessDC[dcindex] = task.uselessDC[dcindex];
			for(int dataindex = 0; dataindex < speculativeTask.numberOfData; dataindex++) {
				speculativeTask.bandwidth[dcindex][dataindex] = task.bandwidth[dcindex][dataindex];
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
//		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
//				+ vm.getId() + " starts executing Task # "
//				+ task.getCloudletId() + " \"" + task.getName() + " "
//				+ task.getParams() + " \"");
		if(log.isInfoEnabled()) {
			log.info(CloudSim.clock() + ": " + getName() + ": VM # "
				+ vm.getId() + " in DC"+ vm.DCId 
				+ " with mips:" + vm.getMips() + " bw:" + vm.getBw()
				+ " starts executing Task # "
				+ task.getCloudletId() + " \"" + task.getName() + " "
				+ task.getParams() + " \"");
		}
		task.setVmId(vm.getId());
		if(Parameters.isHappendUnstable == false) {
			task.setScheduledToFail(false);
		}else {
			if (Parameters.numGen.nextDouble() < getLikelihoodOfFailureOfDC().get(task.getAssignmentDCId())) {
				task.setScheduledToFail(true);
				task.setCloudletLength((long) (task.getCloudletLength() * getRuntimeFactorIncaseOfFailureOfDC().get(task.getAssignmentDCId())));
			} else {
				task.setScheduledToFail(false);
			}
		}
		
		int usedSlots = usedSlotsOfJob.get(task.jobId);
		usedSlotsOfJob.put(task.jobId, usedSlots+1);
		sendNow(getVmsToDatacentersMap().get(vm.getId()),
				CloudSimTags.CLOUDLET_SUBMIT_ACK, task);
	}

	private void submitSpeculativeTask(Task task, Vm vm) {
//		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
//				+ vm.getId() + " starts executing speculative copy of Task # "
//				+ task.getCloudletId() + " \"" + task.getName() + " "
//				+ task.getParams() + " \"");
		if(log.isInfoEnabled()) {
			log.info(CloudSim.clock() + ": " + getName() + ": VM # "
					+ vm.getId() + " in DC"+ vm.DCId 
					+ " with mips:" + vm.getMips() + " bw:" + vm.getBw()
					+ " starts executing speculative copy of Task # "
					+ task.getCloudletId() + " \"" + task.getName() + " "
					+ task.getParams() + " \"");
		}
		task.setVmId(vm.getId());
		if(Parameters.isHappendUnstable == false) {
			task.setScheduledToFail(false);
		}else {
			if (Parameters.numGen.nextDouble() < getLikelihoodOfFailureOfDC().get(task.getAssignmentDCId())) {
				task.setScheduledToFail(true);
				task.setCloudletLength((long) (task.getCloudletLength() * getRuntimeFactorIncaseOfFailureOfDC().get(task.getAssignmentDCId())));
			} else {
				task.setScheduledToFail(false);
			}
		}
		int usedSlots = usedSlotsOfJob.get(task.jobId);
		usedSlotsOfJob.put(task.jobId, usedSlots+1);
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
		int usedSlots = 0;
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
//						Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
//								+ speculativeTask.getVmId()
//								+ " completed speculative copy of Task # "
//								+ speculativeTask.getCloudletId() + " \""
//								+ speculativeTask.getName() + " "
//								+ speculativeTask.getParams() + " \"");
						if(log.isInfoEnabled()) {
							log.info(CloudSim.clock() + ": " + getName() + ": VM # "
									+ speculativeTask.getVmId()
									+ " completed speculative copy of Task # "
									+ speculativeTask.getCloudletId() + " \""
									+ speculativeTask.getName() + " "
									+ speculativeTask.getParams() + " \"");
						}
					} else {
//						Log.printLine(CloudSim.clock() + ": " + getName()
//						+ ": VM # " + speculativeTask.getVmId()
//						+ " cancelled speculative copy of Task # "
//						+ speculativeTask.getCloudletId() + " \""
//						+ speculativeTask.getName() + " "
//						+ speculativeTask.getParams() + " \"");
						if(log.isInfoEnabled()) {
							log.info(CloudSim.clock() + ": " + getName()
							+ ": VM # " + speculativeTask.getVmId()
							+ " cancelled speculative copy of Task # "
							+ speculativeTask.getCloudletId() + " \""
							+ speculativeTask.getName() + " "
							+ speculativeTask.getParams() + " \"");
						}
						vms.get(speculativeTask.getVmId()).getCloudletScheduler()
						.cloudletCancel(speculativeTask.getCloudletId());
					}
					
				}
				
//				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
//						+ originalTask.getVmId() + " cancelled Task # "
//						+ originalTask.getCloudletId() + " \""
//						+ originalTask.getName() + " "
//						+ originalTask.getParams() + " \"");
				if(log.isInfoEnabled()) {
					log.info(CloudSim.clock() + ": " + getName() + ": VM # "
							+ originalTask.getVmId() + " cancelled Task # "
							+ originalTask.getCloudletId() + " \""
							+ originalTask.getName() + " "
							+ originalTask.getParams() + " \"");
				}
				vms.get(originalTask.getVmId()).getCloudletScheduler()
						.cloudletCancel(originalTask.getCloudletId());
			} else {
//				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
//						+ originalTask.getVmId() + " completed Task # "
//						+ originalTask.getCloudletId() + " \""
//						+ originalTask.getName() + " "
//						+ originalTask.getParams() + " \"");
				if(log.isInfoEnabled()) {
					log.info(CloudSim.clock() + ": " + getName() + ": VM # "
							+ originalTask.getVmId() + " completed Task # "
							+ originalTask.getCloudletId() + " \""
							+ originalTask.getName() + " "
							+ originalTask.getParams() + " \"");
				}
				if (speculativeTaskOfTask.size() != 0) {
					
					Iterator<Task> it = speculativeTaskOfTask.iterator();
					while(it.hasNext()) {
						Task speculativeTask = it.next();
//						Log.printLine(CloudSim.clock() + ": " + getName()
//						+ ": VM # " + speculativeTask.getVmId()
//						+ " cancelled speculative copy of Task # "
//						+ speculativeTask.getCloudletId() + " \""
//						+ speculativeTask.getName() + " "
//						+ speculativeTask.getParams() + " \"");
						if(log.isInfoEnabled()) {
							log.info(CloudSim.clock() + ": " + getName()
							+ ": VM # " + speculativeTask.getVmId()
							+ " cancelled speculative copy of Task # "
							+ speculativeTask.getCloudletId() + " \""
							+ speculativeTask.getName() + " "
							+ speculativeTask.getParams() + " \"");
						}
						vms.get(speculativeTask.getVmId()).getCloudletScheduler()
						.cloudletCancel(speculativeTask.getCloudletId());
					}
					
				}
			}

			// free task slots occupied by finished / cancelled tasks
			Vm originalVm = vms.get(originalTask.getVmId());
			taskSucceeded(originalTask, originalVm);
			idleTaskSlotsOfDC.get(originalVm.DCId).add(originalVm);
			usedSlots = usedSlotsOfJob.get(originalTask.jobId);
			usedSlotsOfJob.put(originalTask.jobId, usedSlots-1);
			
			originalTask.usedVM++;
			originalTask.usedVMxTime = originalTask.usedVMxTime + (CloudSim.clock()-originalTask.getExecStartTime());
			
			double earliestStartTime = originalTask.earliestStartTime;
			if(earliestStartTime == -1.0d) {
				earliestStartTime = originalTask.getExecStartTime();
			}else {
				if(originalTask.getExecStartTime()<earliestStartTime) {
					earliestStartTime = originalTask.getExecStartTime();
				}
			}
			
			// return bandwidth
			double Totaldown = 0;
			if(originalTask.isBandwidthCompetitive == false) {
				
				if(originalTask.numberOfTransferData[originalTask.assignmentDCindex] > 0) {
					
					
					
					for(int dataindex = 0; dataindex < originalTask.numberOfData; dataindex++ ) {
						Totaldown += originalTask.requiredBandwidth[dataindex];
						if (originalTask.requiredBandwidth[dataindex] > 0) {
							sendNow(originalTask.positionOfDataID[dataindex], CloudSimTags.UPLINK_RETURN,originalTask.requiredBandwidth[dataindex]);
							double upbandwidth = getUplinkOfDC().get(originalTask.positionOfDataID[dataindex]) + originalTask.requiredBandwidth[dataindex];
							getUplinkOfDC().put(originalTask.positionOfDataID[dataindex], upbandwidth);
						}
						
					}
					
					originalTask.usedBandwidth = originalTask.usedBandwidth + Totaldown;
					originalTask.usedBandxTime = originalTask.usedBandxTime +
							Totaldown*(CloudSim.clock()-originalTask.getExecStartTime());
					
					if (Totaldown > 0) {
						sendNow(originalTask.getAssignmentDCId(), CloudSimTags.DOWNLINK_RETURN, Totaldown);
						double downbandwidth = getDownlinkOfDC().get(originalTask.getAssignmentDCId()) + Totaldown;
						getDownlinkOfDC().put(originalTask.getAssignmentDCId(), downbandwidth);
					}
					
				}
			}
			
			//tasks.remove(originalTask.getCloudletId());
			//taskSucceeded(originalTask, originalVm);
			if (speculativeTaskOfTask.size() != 0) {
				Iterator<Task> it = speculativeTaskOfTask.iterator();
				while(it.hasNext()) {
					Task speculativeTask = it.next();
					Vm speculativeVm = vms.get(speculativeTask.getVmId());
					taskSucceeded(speculativeTask, speculativeVm);
					idleTaskSlotsOfDC.get(speculativeVm.DCId).add(speculativeVm);
					usedSlots = usedSlotsOfJob.get(speculativeTask.jobId);
					usedSlotsOfJob.put(speculativeTask.jobId, usedSlots-1);
					originalTask.usedVM++;
					originalTask.usedVMxTime = originalTask.usedVMxTime + (CloudSim.clock()-speculativeTask.getExecStartTime());
					
					if(speculativeTask.getExecStartTime() < earliestStartTime) {
						earliestStartTime = speculativeTask.getExecStartTime();
					}
					
					// return bandwidth
					if(speculativeTask.isBandwidthCompetitive == false) {
						Totaldown = 0;
						if(speculativeTask.numberOfTransferData[speculativeTask.assignmentDCindex] > 0) {
							
							
							
							for(int dataindex = 0; dataindex < speculativeTask.numberOfData; dataindex++ ) {
								Totaldown += speculativeTask.requiredBandwidth[dataindex];
								if (speculativeTask.requiredBandwidth[dataindex] > 0) {
									sendNow(speculativeTask.positionOfDataID[dataindex], CloudSimTags.UPLINK_RETURN,speculativeTask.requiredBandwidth[dataindex]);
									double upbandwidth = getUplinkOfDC().get(speculativeTask.positionOfDataID[dataindex]) + speculativeTask.requiredBandwidth[dataindex];
									getUplinkOfDC().put(speculativeTask.positionOfDataID[dataindex], upbandwidth);
								}
								
							}
							originalTask.usedBandwidth = originalTask.usedBandwidth + Totaldown;
							originalTask.usedBandxTime = originalTask.usedBandxTime +
									Totaldown*(CloudSim.clock()-speculativeTask.getExecStartTime());
							
							if(Totaldown > 0) {
								sendNow(speculativeTask.getAssignmentDCId(), CloudSimTags.DOWNLINK_RETURN, Totaldown);
								double downbandwidth = getDownlinkOfDC().get(speculativeTask.getAssignmentDCId()) + Totaldown;
								getDownlinkOfDC().put(speculativeTask.getAssignmentDCId(), downbandwidth);
							}
							
						}
						
					}
					
					
					//taskFailed(speculativeTask, speculativeVm);
				}
				//speculativeTasks.remove(originalTask.getCloudletId());
			}
			task.usedVM = originalTask.usedVM;
			task.usedBandwidth = originalTask.usedBandwidth;
			task.usedVMxTime = originalTask.usedVMxTime;
			task.usedBandxTime = originalTask.usedBandxTime;
			task.earliestStartTime = earliestStartTime;
			collectAckTask(task);

			// if the task finished unsuccessfully,
			// if it was a speculative task, just leave it be
			// otherwise, -- if it exists -- the speculative task becomes the
			// new original
		}else {
			
			if(!tasks.containsKey(task.getCloudletId())) {
				return;
			}
			
			LinkedList<Task> speculativeTaskOfTask = (LinkedList<Task>)speculativeTasks.remove(task.getCloudletId());
			
			
			if (task.isSpeculativeCopy()) {
				Task originalTask = tasks.get(task.getCloudletId());
//				Log.printLine(CloudSim.clock()
//						+ ": "
//						+ getName()
//						+ ": VM # "
//						+ task.getVmId()
//						+ " encountered an error with speculative copy of Task # "
//						+ task.getCloudletId() + " \"" + task.getName() + " "
//						+ task.getParams() + " \"");
				if(log.isInfoEnabled()) {
					log.info(CloudSim.clock()
							+ ": "
							+ getName()
							+ ": VM # "
							+ task.getVmId()
							+ " encountered an error with speculative copy of Task # "
							+ task.getCloudletId() + " \"" + task.getName() + " "
							+ task.getParams() + " \"");
				}
				for(int index = 0; index < speculativeTaskOfTask.size(); index++ ) {
					if (speculativeTaskOfTask.get(index).getVmId() == task.getVmId()) {
						Task speculativeTask = speculativeTaskOfTask.get(index);
						idleTaskSlotsOfDC.get(vm.DCId).add(vm);
						usedSlots = usedSlotsOfJob.get(speculativeTask.jobId);
						usedSlotsOfJob.put(speculativeTask.jobId, usedSlots-1);
						originalTask.usedVM++;
						originalTask.usedVMxTime = originalTask.usedVMxTime + (CloudSim.clock()-speculativeTask.getExecStartTime());
						
						if(originalTask.earliestStartTime == -1.0d) {
							originalTask.earliestStartTime = speculativeTask.getExecStartTime();
						}else {
							if(speculativeTask.getExecStartTime() < originalTask.earliestStartTime) {
								originalTask.earliestStartTime = speculativeTask.getExecStartTime();
							}
						}
						
						double Totaldown = 0;
						if(speculativeTask.isBandwidthCompetitive == false) {
							if(speculativeTask.numberOfTransferData[speculativeTask.assignmentDCindex] > 0) {
								
								
								
								for(int dataindex = 0; dataindex < speculativeTask.numberOfData; dataindex++ ) {
									Totaldown += speculativeTask.requiredBandwidth[dataindex];
									if (speculativeTask.requiredBandwidth[dataindex] > 0) {
										sendNow(speculativeTask.positionOfDataID[dataindex], CloudSimTags.UPLINK_RETURN,speculativeTask.requiredBandwidth[dataindex]);
										double upbandwidth = getUplinkOfDC().get(speculativeTask.positionOfDataID[dataindex]) + speculativeTask.requiredBandwidth[dataindex];
										getUplinkOfDC().put(speculativeTask.positionOfDataID[dataindex], upbandwidth);
									}
									
								}
								
								originalTask.usedBandwidth = 
										originalTask.usedBandwidth + Totaldown;
								originalTask.usedBandxTime = originalTask.usedBandxTime + 
										Totaldown*(CloudSim.clock()-speculativeTask.getExecStartTime());
								
								if(Totaldown > 0) {
									sendNow(speculativeTask.getAssignmentDCId(), CloudSimTags.DOWNLINK_RETURN, Totaldown);
									double downbandwidth = getDownlinkOfDC().get(speculativeTask.getAssignmentDCId()) + Totaldown;
									getDownlinkOfDC().put(speculativeTask.getAssignmentDCId(), downbandwidth);
								}
								
							}
						}
						
						speculativeTaskOfTask.remove(index);
						break;
					}
				}
				// delete the fail speculative task from the speculativelist
				speculativeTasks.put(task.getCloudletId(), speculativeTaskOfTask);
				
			} else {
//				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
//						+ task.getVmId() + " encountered an error with Task # "
//						+ task.getCloudletId() + " \"" + task.getName() + " "
//						+ task.getParams() + " \"");
				if(log.isInfoEnabled()) {
					log.info(CloudSim.clock() + ": " + getName() + ": VM # "
							+ task.getVmId() + " encountered an error with Task # "
							+ task.getCloudletId() + " \"" + task.getName() + " "
							+ task.getParams() + " \"");
				}
				
				
				
				Task originalTask = tasks.remove(task.getCloudletId());
				idleTaskSlotsOfDC.get(vm.DCId).add(vm);
				usedSlots = usedSlotsOfJob.get(originalTask.jobId);
				usedSlotsOfJob.put(originalTask.jobId, usedSlots-1);
				
				originalTask.usedVM++;
				originalTask.usedVMxTime = originalTask.usedVMxTime + (CloudSim.clock()-originalTask.getExecStartTime());

				if(originalTask.earliestStartTime == -1.0d) {
					originalTask.earliestStartTime = originalTask.getExecStartTime();
				}else {
					if(originalTask.getExecStartTime() < originalTask.earliestStartTime) {
						originalTask.earliestStartTime = originalTask.getExecStartTime();
					}
				}
				
				
				double Totaldown = 0;
				if(originalTask.isBandwidthCompetitive == false) {
					if(originalTask.numberOfTransferData[originalTask.assignmentDCindex] > 0) {
						for(int dataindex = 0; dataindex < originalTask.numberOfData; dataindex++ ) {
							
							

							
							Totaldown += originalTask.requiredBandwidth[dataindex];
							if (originalTask.requiredBandwidth[dataindex] > 0) {
								sendNow(originalTask.positionOfDataID[dataindex], CloudSimTags.UPLINK_RETURN,originalTask.requiredBandwidth[dataindex]);
								double upbandwidth = getUplinkOfDC().get(originalTask.positionOfDataID[dataindex]) + originalTask.requiredBandwidth[dataindex];
								getUplinkOfDC().put(originalTask.positionOfDataID[dataindex], upbandwidth);
							}
							
						}
						
						originalTask.usedBandwidth = originalTask.usedBandwidth +
								Totaldown;
						originalTask.usedBandxTime = originalTask.usedBandxTime +
								Totaldown * (CloudSim.clock()-originalTask.getExecStartTime());
						
						
						if(Totaldown > 0) {
							sendNow(originalTask.getAssignmentDCId(), CloudSimTags.DOWNLINK_RETURN, Totaldown);
							double downbandwidth = getDownlinkOfDC().get(originalTask.getAssignmentDCId()) + Totaldown;
							getDownlinkOfDC().put(originalTask.getAssignmentDCId(), downbandwidth);
						}
					}
				}
				
				if (speculativeTaskOfTask.size() != 0) {
					Task speculativeTask = speculativeTaskOfTask.remove();
					speculativeTask.usedVM = originalTask.usedVM;
					speculativeTask.usedBandwidth = originalTask.usedBandwidth;
					speculativeTask.usedVMxTime = originalTask.usedVMxTime;
					speculativeTask.usedBandxTime = originalTask.usedBandxTime;
					speculativeTask.earliestStartTime = originalTask.earliestStartTime;
					speculativeTask.setSpeculativeCopy(false);
					
					tasks.put(speculativeTask.getCloudletId(), speculativeTask);
					speculativeTasks.put(speculativeTask.getCloudletId(), speculativeTaskOfTask);
				} else {
					taskFailed(task, vm);
					task.usedVM = originalTask.usedVM;
					task.usedVMxTime = originalTask.usedVMxTime;
					task.usedBandwidth = originalTask.usedBandwidth;
					task.usedBandxTime = originalTask.usedBandxTime;
					task.earliestStartTime = originalTask.earliestStartTime;
					collectAckTask(task);
				}
			}
//			taskFailed(task, vm);
//			idleTaskSlotsOfDC.get(vm.DCId).add(vm);
		}
    }
    
    protected void resetTask(Task task) {
		task.setCloudletFinishedSoFar(0);
		try {
//			double taskExecStarttime = task.getExecStartTime();
//			double taskFinishTime = task.getFinishTime();
			
			task.setScheduledToFail(false);
			task.setCloudletLength(task.oriCloudletLength);
			task.isBandwidthCompetitive = false;
			task.setCloudletStatus(Cloudlet.CREATED);
			task.setExecStartTime(-1.0d);
			task.setFinishTime(-1.0d);
			task.setBw(0l);
			for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
				task.uselessDC[dcindex] = -1;
				task.numberOfTransferData[dcindex] = 0;
				task.TotalTransferDataSize[dcindex] = 0d;
				for(int dataindex = 0; dataindex < task.numberOfData; dataindex++) {
					task.bandwidth[dcindex][dataindex] = 0d;
					task.transferDataSize[dcindex][dataindex] = 0d;
				}
			}
			for(int dataindex = 0; dataindex < task.numberOfData; dataindex++) {
				task.requiredBandwidth[dataindex] = 0d;
				task.positionOfDataID[dataindex] = 0;
			}
			progressScores.put(task, 0d);
			
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
    	    	if(ackTaskOfJob.get(attributedJobId).equals(scheduledTaskOfJob.get(attributedJobId))) {
    	    		//not really update right now, should wait 1 s until many jobs have returned
    	            schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
    	    	}
    		}else {
    			int numOfScheduledTask = scheduledTaskOfJob.get(job.getCloudletId());
    			scheduledTaskOfJob.put(job.getCloudletId(), numOfScheduledTask - 1);
    			job.unscheduledTaskList.add(task);
    			getCloudletList().add(job);
    	    	if(ackTaskOfJob.get(attributedJobId).equals(scheduledTaskOfJob.get(attributedJobId))) {
    	    		//not really update right now, should wait 1 s until many jobs have returned
    	            schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
    	    	}
    		}
//    		schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
    		return ;
    	}
        int alreadyAckTaskNumber = ackTaskOfJob.get(attributedJobId);
        ackTaskOfJob.put(attributedJobId, alreadyAckTaskNumber + 1);
        
        progressScores.remove(task);
        progressRates.remove(task);
        timesTaskHasBeenRunning.remove(task);
        estimatedTimesToCompletion.remove(task);
        
        
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
        	
        	boolean successflag = true;
        	// jobExeStartTime is the earliest Execution time among tasks
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
        			//double taskstarttime = completeTask.getExecStartTime();
        			double taskstarttime = completeTask.earliestStartTime;
            		double taskfinishtime = completeTask.getFinishTime();
            		if(taskstarttime < JobExeStartTime) {
            			JobExeStartTime = taskstarttime;
            		}
            		if(taskfinishtime > JobFinishedTime) {
            			JobFinishedTime = taskfinishtime;
            		}
            		completeJob.successTaskList.add(completeTask);
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
        		completeJob.earliestStartTime = JobExeStartTime;
        	}
        	if(JobFinishedTime != Double.MIN_VALUE) {
        		completeJob.setFinishTime(JobFinishedTime);
        	}
        	
        	getCloudletReceivedList().add(completeJob);
            getCloudletSubmittedList().remove(completeJob);
            
            schedule(this.workflowEngineId, 0.0, CloudSimTags.CLOUDLET_RETURN, completeJob);

//            for(int tindex = 0; tindex < completeJob.getTaskList().size(); tindex++) {
//            	progressScores.remove(completeJob.getTaskList().get(tindex));
//            }
            taskOfJob.remove(attributedJobId);
            ackTaskOfJob.remove(attributedJobId);
            JobFactory.remove(attributedJobId);
            scheduledTaskOfJob.remove(attributedJobId);
            usedSlotsOfJob.remove(attributedJobId);
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
    	for(int key:idleTaskSlotsOfDC.keySet()) {
    		ori_idleTaskSlotsOfDC.put(key, idleTaskSlotsOfDC.get(key).size());
    		slotNum += idleTaskSlotsOfDC.get(key).size();
    	}
    	rescheduleVms(vms.values());
        sendNow(this.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, null);
    }
    
    
	public void rescheduleVms(Collection<Vm> values) {
		// TODO Auto-generated method stub
		speculativeCapAbs = (int) (values.size() * taskSlotsPerVm * speculativeCap + 0.5);
//		for (Vm vm : values) {
//			if (!nSucceededTasksPerVm.containsKey(vm)) {
//				nSucceededTasksPerVm.put(vm, 0d);
//			}
//		}
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
