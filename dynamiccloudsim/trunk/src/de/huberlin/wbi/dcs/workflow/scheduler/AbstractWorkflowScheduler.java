package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;


import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;

import matlabcontrol.MatlabInvocationException;
import matlabcontrol.MatlabProxy;
import matlabcontrol.MatlabProxyFactory;

import matlabcontrol.extensions.MatlabNumericArray;
import matlabcontrol.extensions.MatlabTypeConverter;
// import taskassign.TaskAssign;


public abstract class AbstractWorkflowScheduler extends DatacenterBroker
		implements WorkflowScheduler {

	private List<Workflow> workflows;
	private Map<Integer, Vm> vms;
	private int taskSlotsPerVm;
	//private Queue<Vm> idleTaskSlots;
	private Map<Integer, LinkedList<Vm>> idleTaskSlotsOfDC;
	private double runtime;

	// two collections of tasks, which are currently running;
	// note that the second collections is a subset of the first collection
	private Map<Integer, Task> tasks;
	private Map<Integer, Queue<Task>> speculativeTasks;
	
	private MatlabProxyFactory factory;
	private MatlabProxy proxy;

	public AbstractWorkflowScheduler(String name, int taskSlotsPerVm)
			throws Exception {
		super(name);
		workflows = new ArrayList<>();
		vms = new HashMap<>();
		this.taskSlotsPerVm = taskSlotsPerVm;
		//idleTaskSlots = new LinkedList<>();
		idleTaskSlotsOfDC = new HashMap<>();
		tasks = new HashMap<>();
		speculativeTasks = new HashMap<>();
		factory = new MatlabProxyFactory();
		proxy = factory.getProxy();
		
	}
	
	
	
	
	
	
	
	

	public List<Workflow> getWorkflows() {
		return workflows;
	}

	public int getTaskSlotsPerVm() {
		return taskSlotsPerVm;
	}

	public void submitWorkflow(Workflow workflow) {
		workflows.add(workflow);
	}
	
	public Map<Integer, LinkedList<Vm>> getIdleTaskSlotsOfDC(){
		return idleTaskSlotsOfDC;
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

	/**
	 * Submit cloudlets to the created VMs. This function is called after Vms
	 * have been created.
	 * 
	 * @pre $none
	 * @post $none
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
		for (Workflow workflow : workflows) {
			Collection<Task> tasks = workflow.getTasks();
			// use the index in workflows to indicate the ID of workflow
			reschedule(tasks, vms.values());
			for (Task task : tasks) {
				if (task.readyToExecute()) {
					taskReady(task);
				}
			}
		}
		submitTasks();
	}

	protected void submitTasks() {
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
		
		
		
		
//		TaskAssign taskassign = null;
//		MWNumericArray tasknum = null;
//		MWNumericArray dcnum = null;
//		MWNumericArray probArray = null;
//		MWNumericArray allDuraArray = null;
//		MWNumericArray data = null;
//		MWNumericArray datapos = null;
//		MWNumericArray bandwidth = null;
//		MWNumericArray SlotArray = null;
//		MWNumericArray UpArray = null;
//		MWNumericArray DownArray = null;
//		MWNumericArray iteration_bound = null;
//		Object[] result = null;	/* Stores the result */
//		MWNumericArray x = null;	/* Location of minimal value */
//		MWNumericArray flag = null;	/* solvable flag */
//		double[] xd = null;
//		int flagi = 0;
//		LinkedList<Task> ReadyTasks = (LinkedList<Task>)getTaskQueue();
//		try {
//			
//			taskassign = new TaskAssign();
//			int[] dims = {1,1};
//			tasknum = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
//			dcnum = MWNumericArray.newInstance(dims, MWClassID.INT64, MWComplexity.REAL);
//			iteration_bound = MWNumericArray.newInstance(dims, MWClassID.INT64, MWComplexity.REAL);
//			dims[0] = Parameters.numberOfDC;
//			dims[1] = 4;
//			probArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
//			dims[0] = numberOfTask * Parameters.numberOfDC;
//			allDuraArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
//			dims[0] = 1;
//			dims[1] = numberOfTask;
//			data = MWNumericArray.newInstance(dims, MWClassID.INT64, MWComplexity.REAL);
//			dims[0] = numberOfTask;
//			dims[1] = Parameters.ubOfData;
//			datapos = MWNumericArray.newInstance(dims, MWClassID.INT64, MWComplexity.REAL);
//			dims[0] = numberOfTask * Parameters.numberOfDC;
//			bandwidth = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
//			dims[0] = 1;
//			dims[1] = Parameters.numberOfDC;
//			SlotArray = MWNumericArray.newInstance(dims, MWClassID.INT64, MWComplexity.REAL);
//			UpArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
//			DownArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
//			
//			
//			// set value to the above arguments
//			
//			tasknum.set(1, numberOfTask);
//			dcnum.set(1, Parameters.numberOfDC);
//			iteration_bound.set(1, Parameters.boundOfIter);
//			
//			//probArray
//			for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
//				for (int iterm = 0; iterm < 4; iterm++) {
//					int[] pos = {dcindex + 1,iterm + 1};
//					double value = 0d;
//					switch (iterm) {
//					case 0:
//						value = (1-Parameters.likelihoodOfDCFailure[dcindex])*(1-Parameters.likelihoodOfFailure[dcindex])*(1-Parameters.likelihoodOfStragglerOfDC[dcindex]);
//						break;
//					case 1:
//						value = (1-Parameters.likelihoodOfDCFailure[dcindex])*(1-Parameters.likelihoodOfFailure[dcindex])*Parameters.likelihoodOfStragglerOfDC[dcindex];
//						break;
//					case 2:
//						value = (1-Parameters.likelihoodOfDCFailure[dcindex])*Parameters.likelihoodOfFailure[dcindex];
//						break;
//					case 3:
//						value = Parameters.likelihoodOfDCFailure[dcindex];
//						break;
//					default:
//						break;
//					}
//					probArray.set(pos,value);
//				}
//			}
//			
//			//data datapos 
//			double[] Totaldatasize = new double[numberOfTask];
//			double[][] datasize = new double[numberOfTask][Parameters.ubOfData];
//			
//			int[] pos = new int[2];
//			for (int tindex = 0; tindex < numberOfTask; tindex++) {
//				Task task = ReadyTasks.get(tindex);
//				pos[0] = 1;
//				pos[1] = tindex + 1;
//				data.set(pos, task.numberOfData);
//				Totaldatasize[tindex] = 0d;
//				for(int dataindex = 0; dataindex < task.numberOfData; dataindex++) {
//					pos[0] = tindex + 1;
//					pos[1] = dataindex + 1;
//					datapos.set(pos, task.positionOfData[dataindex]);
//					task.positionOfDataID[dataindex] = task.positionOfData[dataindex] + DCbase;
//					datasize[tindex][dataindex] = task.sizeOfData[dataindex];
//					Totaldatasize[tindex] += task.sizeOfData[dataindex];
//				}
//				ReadyTasks.set(tindex, task);
//			}
//			
//			//bandwidth allDuraArray
//			for (int tindex = 0; tindex < numberOfTask; tindex++) {
//				Task task = ReadyTasks.get(tindex);
//				for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
//					int xindex = tindex*Parameters.numberOfDC + dcindex;
//					pos[0] = 1;
//					pos[1] = tindex + 1;
//					int datanumber = data.getInt(pos);
//					double[] datasizeOfTask = datasize[tindex];
//					double TotaldatasizeOfTask = Totaldatasize[tindex];
//					for(int dataindex = 0; dataindex < datanumber; dataindex++) {
//						pos[0] = tindex + 1;
//						pos[1] = dataindex + 1;
//						if (datapos.getInt(pos) == dcindex) {
//							TotaldatasizeOfTask -= datasizeOfTask[dataindex];
//							datasizeOfTask[dataindex] = 0;
//						}
//					}
//					for(int dataindex = 0; dataindex < datanumber; dataindex++) {
//						pos[0] = xindex + 1;
//						pos[1] = dataindex + 1;
//						bandwidth.set(pos, Parameters.bwBaselineOfDC[dcindex]*datasizeOfTask[dataindex]/TotaldatasizeOfTask);
//					}
//					for (int iterm = 0; iterm < 4; iterm++) {
//						pos[0] = xindex + 1;
//						pos[1] = iterm + 1;
//						double value = 0d;
//						switch (iterm) {
//						case 0:
//							value = task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
//									+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
//									+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
//							break;
//						case 1:
//							value = task.getMi()/(Parameters.MIPSbaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex])
//									+ TotaldatasizeOfTask/(Parameters.bwBaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex])
//									+ 2*task.getIo()/(Parameters.ioBaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex]);
//							break;
//						case 2:
//							value = (Parameters.runtimeFactorInCaseOfFailure[dcindex] + 1)
//									* task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
//									+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
//									+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
//							break;
//						case 3:
//							value = 2
//									* task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
//									+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
//									+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
//							break;
//						default:
//							break;
//						}
//						allDuraArray.set(pos,value);
//					}
//				}
//				
//			}
//			
//			//SlotArray UpArray DownArray
//			
//			for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
//				pos[0] = 1;
//				pos[1] = dcindex + 1;
//				SlotArray.set(pos, idleTaskSlotsOfDC.get(dcindex + DCbase).size());
//				UpArray.set(pos, getUplinkOfDC().get(dcindex + DCbase));
//				DownArray.set(pos, getDownlinkOfDC().get(dcindex + DCbase));
//			}
//			
//			
//			
//			
//			
//			result = taskassign.command(2,tasknum,dcnum,probArray,allDuraArray,data,datapos,bandwidth,SlotArray,UpArray,DownArray,iteration_bound);
//			x = (MWNumericArray)result[0];
//			xd = x.getDoubleData();
//			flag = (MWNumericArray)result[1];
//			flagi = flag.getInt();
//		}catch(Exception e) {
//			System.out.println("Exception: "+e.toString());
//		}finally {
//			MWNumericArray.disposeArray(tasknum);
//			MWNumericArray.disposeArray(dcnum);
//			MWNumericArray.disposeArray(probArray);
//			MWNumericArray.disposeArray(allDuraArray);
//			MWNumericArray.disposeArray(data);
//			MWNumericArray.disposeArray(datapos);
//			MWNumericArray.disposeArray(bandwidth);
//			MWNumericArray.disposeArray(SlotArray);
//			MWNumericArray.disposeArray(UpArray);
//			MWNumericArray.disposeArray(DownArray);
//			MWNumericArray.disposeArray(iteration_bound);
//			MWNumericArray.disposeArray(x);
//			MWNumericArray.disposeArray(flag);
//			if(taskassign != null)
//				taskassign.dispose();
//		}
		
		
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
		
//		while (tasksRemaining() && !idleTaskSlots.isEmpty()) {
//			Vm vm = idleTaskSlots.remove();
//			Task task = getNextTask(vm);
//			// task will be null if scheduler has tasks to be executed, but not
//			// for this VM (e.g., if it abides to a static schedule or this VM
//			// is a straggler, which the scheduler does not want to assign tasks
//			// to)
//			if (task == null) {
//				taskSlotsKeptIdle.add(vm);
//			} else if (tasks.containsKey(task.getCloudletId())) {
//				speculativeTasks.put(task.getCloudletId(),
//						task);
//				submitSpeculativeTask(task, vm);
//			} else {
//				tasks.put(task.getCloudletId(), task);
//				submitTask(task, vm);
//			}
//		}
//		idleTaskSlots.addAll(taskSlotsKeptIdle);
	}

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

	public double getRuntime() {
		return runtime;
	}

	protected void resetTask(Task task) {
		task.setCloudletFinishedSoFar(0);
		try {
			task.setCloudletStatus(Cloudlet.CREATED);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		// determine what kind of task was finished,
		Task task = (Task) ev.getData();
		Vm vm = vms.get(task.getVmId());
		Host host = vm.getHost();

		// if the task finished successfully, cancel its copy and remove them
		// both from internal data structures
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
			

			// update the task queue by traversing the successor nodes in the
			// workflow
			for (DataDependency outgoingEdge : originalTask.getWorkflow()
					.getGraph().getOutEdges(originalTask)) {
				if (host instanceof DynamicHost) {
					DynamicHost dHost = (DynamicHost) host;
					dHost.addFile(outgoingEdge.getFile());
				}
				Task child = originalTask.getWorkflow().getGraph()
						.getDest(outgoingEdge);
				child.decNDataDependencies();
				if (child.readyToExecute()) {
					taskReady(child);
				}
			}

			// if the task finished unsuccessfully,
			// if it was a speculative task, just leave it be
			// otherwise, -- if it exists -- the speculative task becomes the
			// new original
		} else {
			LinkedList<Task> speculativeTaskOfTask = (LinkedList<Task>)speculativeTasks
					.remove(task.getCloudletId());
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
					}
				}
				
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
					resetTask(task);
					taskReady(task);
				}
			}
			idleTaskSlotsOfDC.get(vm.DCId).add(vm);
			taskFailed(task, vm);
		}
		
		int idleTaskslotsSum = 0;
		for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
			idleTaskslotsSum += idleTaskSlotsOfDC.get(dcindex + DCbase).size();
		}
		
		if (tasksRemaining()) {
			submitTasks();
		} else if (idleTaskslotsSum == getVmsCreatedList().size()
				* getTaskSlotsPerVm()) {
			Log.printLine(CloudSim.clock() + ": " + getName()
					+ ": All Tasks executed. Finishing...");
			terminate();
			clearDatacenters();
			try {
				proxy.exit();
			} catch (MatlabInvocationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finishExecution();
			
		}
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

}
