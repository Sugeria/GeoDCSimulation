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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.reclustering.ReclusteringEngine;


import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.examples.WorkflowExample;
//import matlabcontrol.MatlabInvocationException;
import de.huberlin.wbi.dcs.workflow.Task;

/**
 * WorkflowEngine represents a engine acting on behalf of a user. It hides VM
 * management, as vm creation, submission of cloudlets to this VMs and
 * destruction of VMs.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public final class WorkflowEngine extends SimEntity {

    /**
     * The job list.
     */
    protected List<? extends Cloudlet> jobsList;
    /**
     * The job submitted list.
     */
    protected List<? extends Cloudlet> jobsSubmittedList;
    /**
     * The job received list.
     */
    public static List<? extends Cloudlet> jobsReceivedList;
    /**
     * The job submitted.
     */
    protected int jobsSubmitted;
    
    public static int jobsCompleted = 0;
    protected List<? extends Vm> vmList;
    /**
     * The associated scheduler id*
     */
    private List<Integer> schedulerId;
    private List<WorkflowScheduler> scheduler;

    public Map<Integer, Integer> jobSizeOfWorkflow;
    public Map<Integer, Integer> successJobSizeOfWorkflow;
    public Map<Integer, Double> startTimeOfWorkflow;
    public Map<Integer, Double> finishTimeOfWorkflow;
    public Map<Integer, Double> executionTimeOfWorkflow;
    
    public static FileWriter out;
    /**
     * Created a new WorkflowEngine object.
     *
     * @param name name to be associated with this entity (as required by
     * Sim_entity class from simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public WorkflowEngine(String name) throws Exception {
        this(name, 1);
    }

    public WorkflowEngine(String name, int schedulers) throws Exception {
        super(name);

        setJobsList(new ArrayList<>());
        setJobsSubmittedList(new ArrayList<>());
        setJobsReceivedList(new ArrayList<>());

        jobsSubmitted = 0;
        jobsCompleted = 0;

        setSchedulers(new ArrayList<>());
        setSchedulerIds(new ArrayList<>());
        jobSizeOfWorkflow = new HashMap<>();
        successJobSizeOfWorkflow = new HashMap<>();
        startTimeOfWorkflow = new HashMap<>();
        finishTimeOfWorkflow = new HashMap<>();
        executionTimeOfWorkflow = new HashMap<>();
        for (int i = 0; i < schedulers; i++) {
            WorkflowScheduler wfs = new WorkflowScheduler(name + "_Scheduler_" + i,
            		Parameters.taskSlotsPerVm);
            getSchedulers().add(wfs);
            getSchedulerIds().add(wfs.getId());
            wfs.setWorkflowEngineId(this.getId());
        }
        File file = new File("./result/jobcompletioninfo-"+CloudSim.totalRunIndex+".txt");
        try {
        	out = new FileWriter(file);
        }catch (IOException e) {
			// TODO: handle exception
        	e.printStackTrace();
		}
    }

    /**
     * This method is used to send to the broker the list with virtual machines
     * that must be created.
     *
     * @param list the list
     * @param schedulerId the scheduler id
     */
    public void submitVmList(List<? extends Vm> list, int schedulerId) {
        getScheduler(schedulerId).submitVmList(list);
    }

    public void submitVmList(List<? extends Vm> list) {
        //bug here, not sure whether we should have different workflow schedulers
        getScheduler(0).submitVmList(list);
        setVmList(list);
    }
    
    public List<? extends Vm> getAllVmList(){
        if(this.vmList != null && !this.vmList.isEmpty()){
            return this.vmList;
        }
        else{
            List list = new ArrayList();
            for(int i = 0;i < getSchedulers().size();i ++){
                list.addAll(getScheduler(i).getVmList());
            }
            return list;
        }
    }

    /**
     * This method is used to send to the broker the list of cloudlets.
     *
     * @param list the list
     */
    public void submitCloudletList(List<? extends Cloudlet> list) {
        getJobsList().addAll(list);
    }

    /**
     * Processes events available for this Broker.
     *
     * @param ev a SimEvent object
     */
    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            //this call is from workflow scheduler when all vms are created
            case CloudSimTags.CLOUDLET_SUBMIT:
                submitJobs();
                break;
            case CloudSimTags.CLOUDLET_RETURN:
                processJobReturn(ev);
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            case WorkflowSimTags.JOB_SUBMIT:
                processJobSubmit(ev);
                submitJobs();
                break;
            case CloudSimTags.WORKFLOW_INFO:
            	processWorkflowInfo(ev);
            	break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    private void processWorkflowInfo(SimEvent ev) {
//		Map<Integer, Integer> jobSizeInfo = (HashMap<Integer, Integer>)ev.getData();
//		this.jobSizeOfWorkflow.putAll(jobSizeInfo);
//		for(Integer key : jobSizeInfo.keySet()) {
//			this.successJobSizeOfWorkflow.put(key, 0);
//			this.startTimeOfWorkflow.put(key, Double.MAX_VALUE);
//			this.finishTimeOfWorkflow.put(key, Double.MIN_VALUE);
//		}
	}

	/**
     * Process a request for the characteristics of a PowerDatacenter.
     *
     * @param ev a SimEvent object
     */
    protected void processResourceCharacteristicsRequest(SimEvent ev) {
        for (int i = 0; i < getSchedulerIds().size(); i++) {
            schedule(getSchedulerId(i), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
        }
    }

    /**
     * Binds a scheduler with a datacenter.
     *
     * @param datacenterId the data center id
     * @param schedulerId the scheduler id
     */
    public void bindSchedulerDatacenter(int datacenterId, int schedulerId) {
        getScheduler(schedulerId).bindSchedulerDatacenter(datacenterId);
    }

    /**
     * Binds a datacenter to the default scheduler (id=0)
     *
     * @param datacenterId dataceter Id
     */
    public void bindSchedulerDatacenter(int datacenterId) {
        bindSchedulerDatacenter(datacenterId, 0);
    }
   
    /**
     * Process a submit event
     *
     * @param ev a SimEvent object
     */
    protected void processJobSubmit(SimEvent ev) {
        List<? extends Cloudlet> list = (List) ev.getData();
        getJobsList().addAll(list);
    }

    /**
     * Process a job return event.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    protected void processJobReturn(SimEvent ev) {

        Job job = (Job) ev.getData();
        if (job.getCloudletStatus() != Cloudlet.SUCCESS) {
            // Reclusteringengine will add retry job to jobList
            int newId = getJobsList().size() + getJobsSubmittedList().size();
            getJobsList().addAll(ReclusteringEngine.process(job, newId));
            return ;
        }

//        getJobsReceivedList().add(job);
        jobsCompleted += 1;
        getJobsSubmittedList().remove(job);
        
        // save job info
        
        
        
        
        
        jobsSubmitted--;
//        int attributedWorkflowId = job.workflowId;
//        int jobAck = successJobSizeOfWorkflow.get(attributedWorkflowId);
//        jobAck++;
//        successJobSizeOfWorkflow.put(attributedWorkflowId, jobAck);
//        if (jobAck == jobSizeOfWorkflow.get(attributedWorkflowId)) {
//        	if(job.getExecStartTime() < startTimeOfWorkflow.get(attributedWorkflowId)) {
//        		startTimeOfWorkflow.put(attributedWorkflowId, job.getExecStartTime());
//        	}
//        	if(job.getFinishTime() > finishTimeOfWorkflow.get(attributedWorkflowId)) {
//        		finishTimeOfWorkflow.put(attributedWorkflowId, job.getFinishTime());
//        	}
//        	double exeDura = finishTimeOfWorkflow.get(attributedWorkflowId) - startTimeOfWorkflow.get(attributedWorkflowId);
//        	executionTimeOfWorkflow.put(attributedWorkflowId, exeDura);
//        }else {
//        	if(job.getExecStartTime() < startTimeOfWorkflow.get(attributedWorkflowId)) {
//        		startTimeOfWorkflow.put(attributedWorkflowId, job.getExecStartTime());
//        	}
//        	if(job.getFinishTime() > finishTimeOfWorkflow.get(attributedWorkflowId)) {
//        		finishTimeOfWorkflow.put(attributedWorkflowId, job.getFinishTime());
//        	}
//        }
        boolean isAllDCFail = true;
        for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
        	if(getScheduler(0).healthyStateOfDC.get(getScheduler(0).DCbase + dcindex) == true) {
        		isAllDCFail = false;
        		break;
        	}
        }
        WorkflowScheduler.log.info("Resource");
        WorkflowScheduler.log.info(CloudSim.clock + "\t");
        WorkflowScheduler scheduler_0 = scheduler.get(0);
        int DCbase = scheduler_0.DCbase;
        for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
        	double slot = 0;
        	double up = 0;
        	double down = 0;
        	if(scheduler_0.healthyStateOfDC.get(dcindex + DCbase) == true) {
  				slot = scheduler_0.idleTaskSlotsOfDC.get(dcindex + DCbase).size();
//  				slotNum += SlotArray[0][dcindex];
  			}else {
  				slot = 0;
  			}
  			up = scheduler_0.getUplinkOfDC().get(dcindex + DCbase);
  			down = scheduler_0.getDownlinkOfDC().get(dcindex + DCbase);
  			
        	String resourcelog = slot+"\t"+(slot/scheduler_0.ori_idleTaskSlotsOfDC.get(dcindex+DCbase))+"\t"
					+ up+"\t"+(up/scheduler_0.ori_uplinkOfDC.get(dcindex+DCbase))+"\t"
					+ down+"\t"+(down/scheduler_0.ori_downlinkOfDC.get(dcindex+DCbase))+"\t";
			WorkflowScheduler.log.info(resourcelog);
        }
        
        
        
        //save job info
        try {
			out.write(job.getCloudletId()+"\t");
			out.write(job.arrivalTime+"\t");
			out.write(job.earliestStartTime+"\t");
			out.write(job.getFinishTime()+"\t");
			out.write(job.getFlowTime()+"\t");
			double DataSize = 0d;
			for(int tindex = 0; tindex < job.getTaskList().size(); tindex++) {
				Task task = job.getTaskList().get(tindex);
				for(int dataindex = 0; dataindex < task.numberOfData; dataindex++) {
					DataSize += task.sizeOfData[dataindex];
				}
			}
			out.write(DataSize/1024+"\t");
			out.write(job.getTaskList().get(0).getMi()+"\t");
			out.write(job.submitDCIndex+"\t");
			out.write(job.getTaskList().size()+"\t");
			out.write("\r\n");
			for(int tindex = 0; tindex < job.getTaskList().size(); tindex++) {
				Task task = job.getTaskList().get(tindex);
				out.write(task.getCloudletId()+"\t");
				out.write(task.getCloudletLength()+"\t");
				out.write(task.earliestStartTime+"\t");
				out.write(task.getFinishTime()+"\t");
				out.write(task.getActualCPUTime()+"\t");
				out.write(task.usedVM+"\t");
				out.write(task.usedVMxTime+"\t");
				out.write(task.usedBandwidth+"\t");
				out.write(task.usedBandxTime+"\t");
				out.write("\r\n");
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
        
        
        
        // already judge whether the future has workflow
        
        
        
        
        if (getJobsList().isEmpty() && jobsSubmitted == 0 && isAllDCFail == false && CloudSim.futureSize() == 0) {
            
        	
        	if(CloudSim.totalRunIndex < 3) {
//        		List<Job> outputList0 = getJobsReceivedList();
//        		WorkflowExample.sortJobId(outputList0);
//				WorkflowExample.record(outputList0);
//				Parameters.printJobList(outputList0);
				int numberOfSuccessfulJob = jobsCompleted;
				double accumulatedRuntime = Parameters.sumOfJobExecutime/numberOfSuccessfulJob;
        		
				Log.printLine("Average runtime in minutes: " + accumulatedRuntime / 60);
				
				
				try {
					if(!Parameters.isExtracte) {
						ClusteringEngine.out.close();
					}else {
						ClusteringEngine.in.close();
					}
				} catch (IOException e) {
					// TODO: handle exception
					e.printStackTrace();
				}
				
				try {
					out.write(jobsCompleted+"\t");
					out.write("\r\n");
					out.close();
				}catch (IOException e) {
					// TODO: handle exception
					e.printStackTrace();
				}
				
				Parameters.isExtracte = true;
				CloudSim.totalRunIndex = CloudSim.totalRunIndex + 1;
				
				switch (CloudSim.totalRunIndex) {
				case 1:
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = false;
					break;
				case 2:
					Parameters.copystrategy = 1;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 3:
					Parameters.copystrategy = 1;
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = false;
					break;
				case 4:
					Parameters.copystrategy = 5;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 5:
					Parameters.copystrategy = 5;
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = false;
					break;
				case 6:
					Parameters.copystrategy = 2;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 7:
					Parameters.copystrategy = 3;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 8:
					Parameters.copystrategy = 4;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 9:
					Parameters.copystrategy = 2;
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = false;
					break;
				case 10:
					Parameters.copystrategy = 3;
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = false;
					break;
				case 11:
					Parameters.copystrategy = 4;
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = false;
					break;
				case 12:
					Parameters.copystrategy = 0;
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 13:
					Parameters.copystrategy = 1;
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 14:
					Parameters.copystrategy = 5;
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 15:
					Parameters.copystrategy = 0;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 16:
					Parameters.copystrategy = 1;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 17:
					Parameters.copystrategy = 5;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					break;
				case 18:
					Parameters.copystrategy = 0;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = true;
					break;
				case 19:
					Parameters.copystrategy = 1;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = true;
					break;
				case 20:
					Parameters.copystrategy = 5;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = true;
					break;
				case 21:
					Parameters.copystrategy = 0;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = false;
					break;
				case 22:
					Parameters.copystrategy = 1;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = false;
					break;
				case 23:
					Parameters.copystrategy = 5;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = false;
					break;
				case 24:
					Parameters.copystrategy = 0;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.1;
					break;
				case 25:
					Parameters.copystrategy = 1;
					break;
				case 26:
					Parameters.copystrategy = 5;
					break;
				case 27:
					Parameters.copystrategy = 0;
					Parameters.epsilon = 0.2;
					break;
				case 28:
					Parameters.copystrategy = 1;
					break;
				case 29:
					Parameters.copystrategy = 5;
					break;
				case 30:
					Parameters.copystrategy = 0;
					Parameters.epsilon = 0.3;
					break;
				case 31:
					Parameters.copystrategy = 1;
					break;
				case 32:
					Parameters.copystrategy = 5;
					break;
				case 33:
					Parameters.copystrategy = 0;
					Parameters.epsilon = 0.4;
					break;
				case 34:
					Parameters.copystrategy = 1;
					break;
				case 35:
					Parameters.copystrategy = 5;
					break;
				case 36:
					Parameters.copystrategy = 0;
					Parameters.epsilon = 0.5;
					break;
				case 37:
					Parameters.copystrategy = 1;
					break;
				case 38:
					Parameters.copystrategy = 5;
					break;
				case 39:
					Parameters.copystrategy = 0;
					Parameters.epsilon = 0.7;
					break;
				case 40:
					Parameters.copystrategy = 1;
					break;
				case 41:
					Parameters.copystrategy = 5;
					break;
				case 42:
					Parameters.copystrategy = 0;
					Parameters.epsilon = 0.8;
					break;
				case 43:
					Parameters.copystrategy = 1;
					break;
				case 44:
					Parameters.copystrategy = 5;
					break;
				case 45:
					Parameters.copystrategy = 0;
					Parameters.epsilon = 0.9;
					break;
				case 46:
					Parameters.copystrategy = 1;
					break;
				case 47:
					Parameters.copystrategy = 5;
					break;
				case 48:
					Parameters.copystrategy = 0;
					Parameters.epsilon = 1;
					break;
				case 49:
					Parameters.copystrategy = 1;
					break;
				case 50:
					Parameters.copystrategy = 5;
					break;
				case 51:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = false;
					Parameters.epsilon = 0.6d;
					break;
				case 52:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.6d;
					break;
				case 53:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = false;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.6d;
					break;
				case 54:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = false;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.6d;
					break;
				case 55:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = false;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.6d;
					break;
				case 56:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = false;
					Parameters.epsilon = 0.6d;
					break;
				case 57:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.1d;
					break;
				case 58:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.2d;
					break;
				case 59:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.3d;
					break;
				case 60:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.4d;
					break;
				case 61:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.5d;
					break;
				case 62:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.7d;
					break;
				case 63:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.8d;
					break;
				case 64:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 0.9d;
					break;
				case 65:
					Parameters.copystrategy = 6;
					Parameters.isConcernDCFail = true;
					Parameters.isConcernUnstable = true;
					Parameters.isConcernGeoNet = true;
					Parameters.isUselessDCuseful = true;
					Parameters.epsilon = 1d;
					break;
				default:
					break;
				}
				
				
				
				
//				int strategyIndex = (int)Math.floor(((double)CloudSim.totalRunIndex)/((double)Parameters.numberOfRun));
//				int runIndex = (int)(((double)CloudSim.totalRunIndex)%((double)Parameters.numberOfRun));
//				Parameters.copystrategy = strategyIndex;
//				Parameters.runIndex = runIndex;
				
				
				
				
				WorkflowScheduler.log.info("Begin "+CloudSim.clock()+": WorkflowEngine Strategy"+CloudSim.totalRunIndex);
				
				
				CloudSim.clock = 0.1d;
				// clear cloudsim.future
				sendNow(CloudSim.getEntity("planner_0").getId(), CloudSimTags.RUN_INITIAL,null);
				
				
				if(Parameters.isConcernUnstable == false) {
					Parameters.r = 0;
				}
				
				for (int dcindex = 0;dcindex < Parameters.numberOfDC; dcindex++) {
					sendNow(CloudSim.getEntity("Datacenter_"+dcindex).getId(),CloudSimTags.INITIAL_LASTTIME,null);
				}
				
			}else {
				//send msg to all the schedulers
	            for (int i = 0; i < getSchedulerIds().size(); i++) {
	            	getScheduler(i).clearDatacenters();
//	            	try {
//	    				getScheduler(i).proxy.exit();
//	    			} catch (MatlabInvocationException e) {
//	    				e.printStackTrace();
//	    			}
	                sendNow(getSchedulerId(i), CloudSimTags.END_OF_SIMULATION, null);
	            }
			}
        	 
        } else {
            sendNow(this.getId(), CloudSimTags.CLOUDLET_SUBMIT, null);
        }
    }

    /**
     * Overrides this method when making a new and different type of Broker.
     * This method is called by {@link #body()} for incoming unknown tags.
     *
     * @param ev a SimEvent object
     */
    protected void processOtherEvent(SimEvent ev) {
        if (ev == null) {
            Log.printLine(getName() + ".processOtherEvent(): " + "Error - an event is null.");
            return;
        }
        Log.printLine(getName() + ".processOtherEvent(): "
                + "Error - event unknown by this DatacenterBroker.");
    }

    /**
     * Checks whether a job list contains a id
     *
     * @param jobList the job list
     * @param id the job id
     * @return
     */
    private boolean hasJobListContainsID(List jobList, int id) {
        for (Iterator it = jobList.iterator(); it.hasNext();) {
            Job job = (Job) it.next();
            if (job.getCloudletId() == id) {
                return true;
            }
        }
        return false;
    }

    /**
     * Submit jobs to the created VMs.
     *
     * @pre $none
     * @post $none
     */
    protected void submitJobs() {

        List<Job> list = getJobsList();
        Map<Integer, List> allocationList = new HashMap<>();
        for (int i = 0; i < getSchedulers().size(); i++) {
            List<Job> submittedList = new ArrayList<>();
            allocationList.put(getSchedulerId(i), submittedList);
        }
        int num = list.size();
        for (int i = 0; i < num; i++) {
            //at the beginning
            Job job = list.get(i);
            //Dont use job.isFinished() it is not right
            if (!hasJobListContainsID(this.getJobsReceivedList(), job.getCloudletId())) {
                List<Job> parentList = job.getParentList();
                boolean flag = true;
                for (Job parent : parentList) {
                    if (!hasJobListContainsID(this.getJobsReceivedList(), parent.getCloudletId())) {
                        flag = false;
                        break;
                    }
                }
                /**
                 * This job's parents have all completed successfully. Should
                 * submit.
                 */
                if (flag) {
                    List submittedList = allocationList.get(job.getUserId());
                    submittedList.add(job);
                    jobsSubmitted++;
                    getJobsSubmittedList().add(job);
                    list.remove(job);
                    i--;
                    num--;
                }
            }

        }
        /**
         * If we have multiple schedulers. Divide them equally.
         */
        for (int i = 0; i < getSchedulers().size(); i++) {

            List submittedList = allocationList.get(getSchedulerId(i));
            //divid it into sublist

            int interval = Parameters.getOverheadParams().getWEDInterval();
            double delay = 0.0;
            if(Parameters.getOverheadParams().getWEDDelay()!=null){
                delay = Parameters.getOverheadParams().getWEDDelay(submittedList);
            }

            double delaybase = delay;
            int size = submittedList.size();
            if (interval > 0 && interval <= size) {
                int index = 0;
                List subList = new ArrayList();
                while (index < size) {
                    subList.add(submittedList.get(index));
                    index++;
                    if (index % interval == 0) {
                        //create a new one
                        schedule(getSchedulerId(i), delay, CloudSimTags.CLOUDLET_SUBMIT, subList);
                        delay += delaybase;
                        subList = new ArrayList();
                    }
                }
                if (!subList.isEmpty()) {
                    schedule(getSchedulerId(i), delay, CloudSimTags.CLOUDLET_SUBMIT, subList);
                }
            } else if (!submittedList.isEmpty()) {
                sendNow(this.getSchedulerId(i), CloudSimTags.CLOUDLET_SUBMIT, submittedList);
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
        Log.printLine(getName() + " is shutting down...");
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#startEntity()
     * Here we creata a message when it is started
     */
    @Override
    public void startEntity() {
        Log.printLine(getName() + " is starting...");
        //schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
    }

    /**
     * Gets the job list.
     *
     * @param <T> the generic type
     * @return the job list
     */
    @SuppressWarnings("unchecked")
    public <T extends Cloudlet> List<T> getJobsList() {
        return (List<T>) jobsList;
    }

    /**
     * Sets the job list.
     *
     * @param <T> the generic type
     * @param cloudletList the new job list
     */
    public <T extends Cloudlet> void setJobsList(List<T> jobsList) {
        this.jobsList = jobsList;
    }

    /**
     * Gets the job submitted list.
     *
     * @param <T> the generic type
     * @return the job submitted list
     */
    @SuppressWarnings("unchecked")
    public <T extends Cloudlet> List<T> getJobsSubmittedList() {
        return (List<T>) jobsSubmittedList;
    }

    /**
     * Sets the job submitted list.
     *
     * @param <T> the generic type
     * @param jobsSubmittedList the new job submitted list
     */
    public <T extends Cloudlet> void setJobsSubmittedList(List<T> jobsSubmittedList) {
        this.jobsSubmittedList = jobsSubmittedList;
    }

    /**
     * Gets the job received list.
     *
     * @param <T> the generic type
     * @return the job received list
     */
    @SuppressWarnings("unchecked")
    public static <T extends Cloudlet> List<T> getJobsReceivedList() {
        return (List<T>) jobsReceivedList;
    }

    /**
     * Sets the job received list.
     *
     * @param <T> the generic type
     * @param cloudletReceivedList the new job received list
     */
    public <T extends Cloudlet> void setJobsReceivedList(List<T> jobsReceivedList) {
        this.jobsReceivedList = jobsReceivedList;
    }

    /**
     * Gets the vm list.
     *
     * @param <T> the generic type
     * @return the vm list
     */
    @SuppressWarnings("unchecked")
    public <T extends Vm> List<T> getVmList() {
        return (List<T>) vmList;
    }

    /**
     * Sets the vm list.
     *
     * @param <T> the generic type
     * @param vmList the new vm list
     */
    private <T extends Vm> void setVmList(List<T> vmList) {
        this.vmList = vmList;
    }

    /**
     * Gets the schedulers.
     *
     * @return the schedulers
     */
    public List<WorkflowScheduler> getSchedulers() {
        return this.scheduler;
    }

    /**
     * Sets the scheduler list.
     *
     * @param <T> the generic type
     * @param vmList the new scheduler list
     */
    private void setSchedulers(List list) {
        this.scheduler = list;
    }

    /**
     * Gets the scheduler id.
     *
     * @return the scheduler id
     */
    public List<Integer> getSchedulerIds() {
        return this.schedulerId;
    }

    /**
     * Sets the scheduler id list.
     *
     * @param <T> the generic type
     * @param vmList the new scheduler id list
     */
    private void setSchedulerIds(List list) {
        this.schedulerId = list;
    }

    /**
     * Gets the scheduler id list.
     *
     * @param index
     * @return the scheduler id list
     */
    public int getSchedulerId(int index) {
        if (this.schedulerId != null) {
            return this.schedulerId.get(index);
        }
        return 0;
    }

    /**
     * Gets the scheduler .
     *
     * @param schedulerId
     * @return the scheduler
     */
    public WorkflowScheduler getScheduler(int schedulerId) {
        if (this.scheduler != null) {
            return this.scheduler.get(schedulerId);
        }
        return null;
    }
}
