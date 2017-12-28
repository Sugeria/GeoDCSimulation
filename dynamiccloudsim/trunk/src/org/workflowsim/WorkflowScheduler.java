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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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


import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.examples.Parameters.SchedulingAlgorithm;
import de.huberlin.wbi.dcs.workflow.Task;
import matlabcontrol.MatlabInvocationException;
import matlabcontrol.MatlabProxy;
import matlabcontrol.MatlabProxyFactory;
import matlabcontrol.extensions.MatlabNumericArray;
import matlabcontrol.extensions.MatlabTypeConverter;
import taskassign.TaskAssign;

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
	private Map<Integer, Job> JobFactory;
	private double runtime;
	
	private MatlabProxyFactory factory;
	public MatlabProxy proxy;

	
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
		JobFactory = new HashMap<>();
		factory = new MatlabProxyFactory();
		proxy = factory.getProxy();
		
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
        scheduler.setVmList(getVmsCreatedList());

        taskAssign = new TaskAssign();
        try {
        	scheduler.taskAssign = taskAssign;
            scheduler.run();
        } catch (Exception e) {
            Log.printLine("Error in configuring scheduler_method");
            e.printStackTrace();
        }

        List<Cloudlet> rankedList = scheduler.getRankedList();
//        for (Cloudlet cloudlet : rankedList) {
        	
    	// execute the algorithm assign tasks
    	// if the resource is not enough for all the jobs then delete the last one in rankedList
    	int loopNumber = rankedList.size();
    	for (int loopindex = 0; loopindex < loopNumber; loopindex++) {
    		// add all the tasks in each job into TaskQueue
    		for (int jobindex = 0; jobindex < (rankedList.size() - loopindex); jobindex++) {
    			Job job = (Job)rankedList.get(jobindex);
    			
    			List<Task> tasklist = job.getTaskList();
    			if(tasklist.size() == 0) {
    				job.jobId = job.getCloudletId();
    				taskReady(job);
    			}else {
    				for (int taskindex = 0; taskindex < tasklist.size();taskindex++) {
        				Task task = tasklist.get(taskindex);
        				taskReady(task);
        			}
    			}
    			
    		}
    		if (tasksRemaining()) {
    			int isResourceEnough = submitTasks();
        		if (isResourceEnough > 0) {
        			for (int jobindex = 0; jobindex < (rankedList.size() - loopindex); jobindex++) {
            			Job job = (Job)rankedList.get(jobindex);
            			
            			
            			if(!taskOfJob.containsKey(job.getCloudletId())) {
            				if(job.getTaskList().size() == 0) {
            					job.jobId = job.getCloudletId();
                				taskOfJob.put(job.getCloudletId(), 1);
                				ackTaskOfJob.put(job.getCloudletId(), 0);
                				JobFactory.put(job.getCloudletId(), job);
                			}else {
                				// when return remember to delete the item in the three tables
                				taskOfJob.put(job.getCloudletId(), job.getTaskList().size());
                				ackTaskOfJob.put(job.getCloudletId(), 0);
                				JobFactory.put(job.getCloudletId(), job);
                			}
            				
            			}
            			scheduler.getScheduledList().add(job);
        			}
        			break;
        		}else {
        			getTaskQueue().clear();
        		}
    		}
    	}
        	
//        }
        getCloudletList().removeAll(scheduler.getScheduledList());
        getCloudletSubmittedList().addAll(scheduler.getScheduledList());
        cloudletsSubmitted += scheduler.getScheduledList().size();
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
    
    protected int submitTasksViaServiceRate() {
    	
    	// Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
		Queue<Task> taskSubmitted = new LinkedList<>();
		// compute the task assignment among datacenters for ready tasks
		
		Integer numberOfTask = getTaskQueue().size();
		
		int tasknum = numberOfTask;
		int dcnum = Parameters.numberOfDC;
		int iteration_bound = Parameters.boundOfIter;
		double[][] probArray = new double[Parameters.numberOfDC][4];
		double[][] allDuraArray = new double[numberOfTask*Parameters.numberOfDC][4];
		int[] data = new int[numberOfTask];
		double[][] datapos = new double[numberOfTask][Parameters.ubOfData];
		double[][] bandwidth = new double[numberOfTask*Parameters.numberOfDC][Parameters.ubOfData];
		double[][] SlotArray = new double[1][Parameters.numberOfDC];
		double[][] UpArray = new double[1][Parameters.numberOfDC];
		double[][] DownArray = new double[1][Parameters.numberOfDC];

    	// obtain the expectation for all the tasks in each datacenter
		// using matlab JavaBuild
		
    	
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
    
    
	protected int submitTasks() {
		// Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
		Queue<Task> taskSubmitted = new LinkedList<>();
		// compute the task assignment among datacenters for ready tasks
		Integer numberOfTask = getTaskQueue().size();
		
		int tasknum = numberOfTask;
		int dcnum = Parameters.numberOfDC;
		int iteration_bound = Parameters.boundOfIter;
		double[][] probArray = new double[Parameters.numberOfDC][4];
		double[][] allDuraArray = new double[numberOfTask*Parameters.numberOfDC][4];
		int[] data = new int[numberOfTask];
		double[][] datapos = new double[numberOfTask][Parameters.ubOfData];
		double[][] bandwidth = new double[numberOfTask*Parameters.numberOfDC][Parameters.ubOfData];
		double[][] SlotArray = new double[1][Parameters.numberOfDC];
		double[][] UpArray = new double[1][Parameters.numberOfDC];
		double[][] DownArray = new double[1][Parameters.numberOfDC];
		double[] xb = null;
		int flagi = 0;
		LinkedList<Task> ReadyTasks = (LinkedList<Task>)getTaskQueue();
		
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
		
		for (int tindex = 0; tindex < numberOfTask; tindex++) {
			Task task = ReadyTasks.get(tindex);
			data[tindex] = task.numberOfData;
			Totaldatasize[tindex] = 0d;
			
			for(int dataindex = 0; dataindex < task.numberOfData; dataindex++) {
				datapos[tindex][dataindex] = task.positionOfData[dataindex];
				task.positionOfDataID[dataindex] = task.positionOfData[dataindex] + DCbase;
				datasize[tindex][dataindex] = task.sizeOfData[dataindex];
				Totaldatasize[tindex] += task.sizeOfData[dataindex];
			}
			ReadyTasks.set(tindex, task);
		}
		
		//bandwidth allDuraArray
		for (int tindex = 0; tindex < numberOfTask; tindex++) {
			Task task = ReadyTasks.get(tindex);
			for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
				int xindex = tindex*Parameters.numberOfDC + dcindex;
				int numberOfTransferData = 0;
				int datanumber = (int)data[tindex];
				double[] datasizeOfTask = datasize[tindex];
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
				}
				
				for(int dataindex = 0; dataindex < datanumber; dataindex++) {
					if (TotaldatasizeOfTask > 0) {
						bandwidth[xindex][dataindex] = Parameters.bwBaselineOfDC[dcindex]*datasizeOfTask[dataindex]/TotaldatasizeOfTask;
						
					}else {
						bandwidth[xindex][dataindex] = 0;
					}
				}
				for (int iterm = 0; iterm < 4; iterm++) {
					double value = 0d;
					switch (iterm) {
					case 0:
						value = task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
								+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
								+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
						break;
					case 1:
						value = task.getMi()/(Parameters.MIPSbaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex])
								+ TotaldatasizeOfTask/(Parameters.bwBaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex])
								+ 2*task.getIo()/(Parameters.ioBaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex]);
						break;
					case 2:
						value = (Parameters.runtimeFactorInCaseOfFailure[dcindex] + 1)
								* task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
								+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
								+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
						break;
					case 3:
						value = 2
								* task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
								+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
								+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
						break;
					default:
						break;
					}
					allDuraArray[xindex][iterm] = value;
				}
			}
			ReadyTasks.set(tindex, task);
			
		}
		
		//SlotArray UpArray DownArray
		
		for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
			if(healthyStateOfDC.get(dcindex + DCbase) == true) {
				SlotArray[0][dcindex] = idleTaskSlotsOfDC.get(dcindex + DCbase).size();
			}else {
				SlotArray[0][dcindex] = 0;
			}
			UpArray[0][dcindex] = getUplinkOfDC().get(dcindex + DCbase);
			DownArray[0][dcindex] = getDownlinkOfDC().get(dcindex + DCbase);
		}
		
		try {
			
			MatlabTypeConverter processor = new MatlabTypeConverter(proxy);
			processor.setNumericArray("probArray", new MatlabNumericArray(probArray, null));
			processor.setNumericArray("allDuraArray", new MatlabNumericArray(allDuraArray, null));
			processor.setNumericArray("bandwidth", new MatlabNumericArray(bandwidth,null));
			processor.setNumericArray("SlotArray", new MatlabNumericArray(SlotArray, null));
			processor.setNumericArray("UpArray", new MatlabNumericArray(UpArray, null));
			processor.setNumericArray("DownArray", new MatlabNumericArray(DownArray,null));
			processor.setNumericArray("datapos", new MatlabNumericArray(datapos, null));
			proxy.setVariable("tasknum", tasknum);
			proxy.setVariable("dcnum", dcnum);
			proxy.setVariable("iteration_bound", iteration_bound);
			proxy.setVariable("data", data);
			
			
		//	proxy.eval("[x,flag] = command(tasknum,dcnum,probArray,allDuraArray,data,datapos,bandwidth,SlotArray,UpArray,DownArray,iteration_bound);");
			proxy.eval("[x,flag] = commandwithnocopy(tasknum,dcnum,probArray,allDuraArray,data,datapos,bandwidth,SlotArray,UpArray,DownArray,iteration_bound);");
			
			xb = (double[])proxy.getVariable("x");
			flagi = (int)((double[])proxy.getVariable("flag"))[0];
			
		} catch (MatlabInvocationException e) {
			e.printStackTrace();
		}
		
		
		
		
		if (flagi > 0) {
			// successful assignment
			for (int tindex = 0; tindex < numberOfTask; tindex++) {
				Task task = ReadyTasks.get(tindex);
				for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
					if (xb[tindex*Parameters.numberOfDC + dcindex] == 1) {
						Vm vm = idleTaskSlotsOfDC.get(dcindex + DCbase).remove();
						if (tasks.containsKey(task.getCloudletId())) {
							Task speculativeTask = new Task(task);
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
				taskSubmitted.add(task);
			}
			getTaskQueue().removeAll(taskSubmitted);
		}
		
		return flagi;
		
	}
    
    
    
    
    
    
    

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
    	int attributedJobId = task.jobId;
        int alreadyAckTaskNumber = ackTaskOfJob.get(attributedJobId);
        ackTaskOfJob.put(attributedJobId, alreadyAckTaskNumber + 1);
        Job job = JobFactory.get(attributedJobId);
    	List<Task> originalTaskList = job.getTaskList();
    	for(int taskindex = 0; taskindex < originalTaskList.size(); taskindex++) {
    		if(originalTaskList.get(taskindex).getCloudletId() == task.getCloudletId()) {
    			originalTaskList.set(taskindex, task);
    			break;
    		}
    	}
    	job.setTaskList(originalTaskList);
    	JobFactory.put(job.getCloudletId(), job);
        if(ackTaskOfJob.get(attributedJobId) == taskOfJob.get(attributedJobId)) {
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
                    JobFactory.remove(attributedJobId);
                    
                    cloudletsSubmitted--;
                    //not really update right now, should wait 1 s until many jobs have returned
                    schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
                }else {
                	resetTask(completeJob);
                    schedule(this.workflowEngineId, 0.0, CloudSimTags.CLOUDLET_RETURN, completeJob);

                    taskOfJob.remove(attributedJobId);
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
        		
        		if (successflag == true && completeTask.getStatus() != Cloudlet.SUCCESS) {
        			successflag = false;
        			try {
						completeJob.setCloudletStatus(Cloudlet.FAILED);
					} catch (Exception e) {
						e.printStackTrace();
					}
        		}
        		
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
