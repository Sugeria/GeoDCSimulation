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
package org.workflowsim.clustering;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.UtilizationModelFull;
import org.workflowsim.ClusteringEngine;
import org.workflowsim.FileItem;
import org.workflowsim.Job;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.examples.Parameters.ClassType;
import de.huberlin.wbi.dcs.examples.Parameters.FileType;
import de.huberlin.wbi.dcs.workflow.Task;
import edu.isi.pegasus.planner.transfer.refiner.Cluster;

/**
 * The default clustering does no clustering at all, just map a task to a tasks
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class BasicClustering implements ClusteringInterface {

	public int userId;
    /**
     * The task list.
     */
    private List<Task> taskList;
    /**
     * The tasks list.
     */
    private final List<Job> jobList;
    /**
     * maps from task to its tasks.
     */
    private final Map mTask2Job;
    /**
     * All the files.
     */
    private final List<FileItem> allFileList;
    /**
     * The root task.
     */
    private Task root;
    /**
     * the id index.
     */
    public static int idIndex = 0;

   
    
    
    /**
     * Gets the files
     *
     * @return files
     */
    @Override
    public final List<FileItem> getTaskFiles() {
        return this.allFileList;
    }

    /**
     * Initialize a BasicClustering object
     */
    public BasicClustering() {
        this.jobList = new ArrayList<>();
        this.taskList = new ArrayList<>();
        this.mTask2Job = new HashMap<>();
        this.allFileList = new ArrayList<>();
 //       this.idIndex = 0;
        this.root = null;
    }

    /**
     * Sets the task list
     *
     * @param list task list
     */
    @Override
    public final void setTaskList(List<Task> list) {
        this.taskList = list;
    }

    /**
     * Gets the tasks list
     *
     * @return tasks list
     */
    @Override
    public final List<Job> getJobList() {
        return this.jobList;
    }

    /**
     * Gets the task list
     *
     * @return task list
     */
    @Override
    public final List<Task> getTaskList() {
        return this.taskList;
    }

    /**
     * Gets the map from task to tasks
     *
     * @return map
     */
    public final Map getTask2Job() {
        return this.mTask2Job;
    }

    /**
     * The main function of BasicClustering
     */
    @Override
    public void run() {
        getTask2Job().clear();
        for (Task task : getTaskList()) {
            List<Task> list = new ArrayList<>();
            list.add(task);
            Job job = addTasks2Job(list);
            job.setVmId(task.getVmId());
            getTask2Job().put(task, job);
        }
        /**
         * Handle the dependencies issue.
         */
        updateDependencies();

    }

    /**
     * Add a task to a new tasks
     *
     * @param task the task
     * @return tasks the newly created tasks
     */
    protected final Job addTasks2Job(Task task) {
        List<Task> tasks = new ArrayList<>();
        tasks.add(task);
        return addTasks2Job(tasks);
    }

    /**
     * Add a list of task to a new tasks
     *
     * @param taskList the task list
     * @return tasks the newly created tasks
     */
    protected final Job addTasks2Job(List<Task> taskList) {
    	String line;
    	String[] para_string;
        if (taskList != null && !taskList.isEmpty()) {
            int length = 0;

            int priority = 0;
            int depth = 0;
            /// a bug of cloudsim makes it final of input file size and output file size
            Job job = new Job(userId,idIndex,length/*, inputFileSize, outputFileSize*/);
            job.setClassType(ClassType.COMPUTE.value);
            // set taskset RemoteData
            
            int numberofData = 0;
            int[] positionOfData = null;
			int[] sizeOfData = null;
			long remoteInputSize = 0;
			if(!Parameters.isExtracte) {
				numberofData = (int)Math.round(Math.random()*Parameters.ubOfData);
				try {
					ClusteringEngine.out.write(numberofData+"\t");
					ClusteringEngine.out.write("\r\n");
				} catch (IOException e) {
					
					e.printStackTrace();
				}
				
            }else {
            	try {
					line = ClusteringEngine.in.readLine();
					para_string = line.split("\t");
					numberofData = Integer.parseInt(para_string[0]);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}catch (NullPointerException e) {
					// TODO: handle exception
					e.printStackTrace();
				}
            }
			
			if (numberofData != 0) {
				
				positionOfData = new int[numberofData];
				
				sizeOfData = new int[numberofData];
				
				if(!Parameters.isExtracte) {
					try {
						for (int dataindex = 0; dataindex < numberofData; dataindex++) {
							//positionOfData[dataindex] = ((int)((Math.random()*Parameters.numberOfDC)) % Parameters.numberOfDC);
							sizeOfData[dataindex] = (int)(Math.random()*(Parameters.ubOfDataSize - Parameters.lbOfDataSize ) + Parameters.lbOfDataSize);
							
							ClusteringEngine.out.write(sizeOfData[dataindex]+"\t");
							
							remoteInputSize += sizeOfData[dataindex];
						}
						ClusteringEngine.out.write("\r\n");
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}else {
					try {
						line = ClusteringEngine.in.readLine();
						para_string = line.split("\t");
						for (int dataindex = 0; dataindex < numberofData; dataindex++) {
							//positionOfData[dataindex] = ((int)((Math.random()*Parameters.numberOfDC)) % Parameters.numberOfDC);
							sizeOfData[dataindex] = Integer.parseInt(para_string[dataindex]);
							remoteInputSize += sizeOfData[dataindex];
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
				}
				
				
			}
			
			
			
			// choose the one milength of task to replace all the mi in the same taskset
			int representTaskIndex = (int)(Math.random()*(taskList.size()-1));
			long representMiLength = taskList.get(representTaskIndex).getMi();
			
			List<Integer> dataSrcCandidates = new ArrayList<>();
			
			if(!Parameters.isExtracte) {
				for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
					if(Parameters.delayAmongDCIndex[dcindex][taskList.get(0).submitDCIndex] < 1e20d) {
						dataSrcCandidates.add(dcindex);
					}
				}
				
			}
			
			
			
			
            for (Task task : taskList) {
                length += task.getCloudletLength();

                priority = task.getPriority();
                depth = task.getDepth();
                List<FileItem> fileList = task.getFileList();
                job.getTaskList().add(task);
                task.jobId = job.getCloudletId();
                task.setMi(representMiLength > 0 ? representMiLength : 1);
                
                if(numberofData != 0) {
                	try {
                    	if(!Parameters.isExtracte) {
                    		for (int dataindex = 0; dataindex < numberofData; dataindex++) {
                    			int datasrcindex = (int)(Math.random()*(dataSrcCandidates.size()-1));
                    			positionOfData[dataindex] = dataSrcCandidates.get(datasrcindex);
            					//positionOfData[dataindex] = ((int)((Math.random()*Parameters.numberOfDC)) % Parameters.numberOfDC);
            					ClusteringEngine.out.write(positionOfData[dataindex]+"\t");
                    		}
                    		ClusteringEngine.out.write("\r\n");
                    	}else {
                    		line = ClusteringEngine.in.readLine();
                    		para_string = line.split("\t");
                    		for (int dataindex = 0; dataindex < numberofData; dataindex++) {
                    			positionOfData[dataindex] = Integer.parseInt(para_string[dataindex]);
//                    			int datasrcindex = (int)(Math.random()*(dataSrcCandidates.size()-1));
//                    			positionOfData[dataindex] = dataSrcCandidates.get(datasrcindex);
                    		}
                    	}
    					
    				} catch (IOException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}catch (NullPointerException e) {
						// TODO: handle exception
    					e.printStackTrace();
					}
                    
                }
                
                
                
                //if (numberofData != 0) {
//            	task.incBw(remoteInputSize/1024);
            	task.incIo(remoteInputSize/1024);
            	task.oriCloudletLength = task.getCloudletLength();
				task.numberOfData = numberofData;
				task.positionOfData = new int[numberofData];
				task.sizeOfData = new int[numberofData];
				for(int dataindex = 0; dataindex < numberofData; dataindex++) {
					task.positionOfData[dataindex] = positionOfData[dataindex];
					task.sizeOfData[dataindex] = sizeOfData[dataindex];
				}
				
				task.requiredBandwidth = new double[numberofData];
				task.positionOfDataID = new int[numberofData];
				task.numberOfTransferData = new int[Parameters.numberOfDC];
				task.TotalTransferDataSize = new double[Parameters.numberOfDC];
				task.transferDataSize = new double[Parameters.numberOfDC][Parameters.ubOfData];
				task.orderedDClist = new int[Parameters.numberOfDC];
				task.bandwidth = new double[Parameters.numberOfDC][Parameters.ubOfData];
				task.uselessDC = new int[Parameters.numberOfDC];
				task.usedVM = 0;
				task.usedBandwidth = 0d;
				task.usedVMxTime = 0d;
				task.usedBandxTime = 0d;
				task.earliestStartTime = -1.0d;
				//}
                getTask2Job().put(task, job);
                for (FileItem file : fileList) {
                    boolean hasFile = job.getFileList().contains(file);
                    if (!hasFile) {
                        job.getFileList().add(file);
                        if (file.getType() == FileType.INPUT) {
                            //for stag-in jobs to be used
                            if (!this.allFileList.contains(file)) {
                                this.allFileList.add(file);
                            }
                        } else if (file.getType() == FileType.OUTPUT) {
                            this.allFileList.add(file);
                        }
                    }
                }
                for (String fileName : task.getRequiredFiles()) {
                    if (!job.getRequiredFiles().contains(fileName)) {
                        job.getRequiredFiles().add(fileName);
                    }
                }
            }

            job.setCloudletLength(length);
            job.setDepth(depth);
            job.setPriority(priority);
            job.workflowId = taskList.get(0).workflowId;
            job.unscheduledTaskList.addAll(job.getTaskList());
            job.arrivalTime = job.getTaskList().get(0).arrivalTime;
            job.submitDCIndex = job.getTaskList().get(0).submitDCIndex;
            idIndex++;
            getJobList().add(job);
            return job;
        }

        return null;
    }

    /**
     * For a clustered tasks, we should add clustering delay (by default it is
 zero)
     */
    public void addClustDelay() {

        for (Job job : getJobList()) {
            double delay = Parameters.getOverheadParams().getClustDelay(job);
            delay *= 1000; // the same ratio used when you parse a workflow
            long length = job.getCloudletLength();
            length += (long) delay;
            job.setCloudletLength(length);
        }
    }

    /**
     * Update the dependency issues between tasks/jobs
     */
    protected final void updateDependencies() {
        for (Task task : getTaskList()) {
            Job job = (Job) getTask2Job().get(task);
            for (Task parentTask : task.getParentList()) {
                Job parentJob = (Job) getTask2Job().get(parentTask);
                if (!job.getParentList().contains(parentJob) && parentJob != job) {//avoid dublicate
                    job.addParent(parentJob);
                }
            }
            for (Task childTask : task.getChildList()) {
                Job childJob = (Job) getTask2Job().get(childTask);
                if (!job.getChildList().contains(childJob) && childJob != job) {//avoid dublicate
                    job.addChild(childJob);
                }
            }
        }
        getTask2Job().clear();
        getTaskList().clear();
    }
    /*
     * Add a fake root task
     * If you have used addRoot, please use clean() after that
     */

    public Task addRoot() {

        if (root == null) {
            //bug maybe
            root = new Task("fake_root","null",0,taskList.size() + 1,1,0,0,1,0,0,new UtilizationModelFull(),new UtilizationModelFull(),new UtilizationModelFull(),0);
            for (Task node : taskList) {
                if (node.getParentList().isEmpty()) {
                    node.addParent(root);
                    root.addChild(node);
                }
            }
            taskList.add(root);

        }
        return root;
    }

    /**
     * Delete the root task
     */
    public void clean() {
        if (root != null) {
            for (int i = 0; i < root.getChildList().size(); i++) {
                Task node = (Task) root.getChildList().get(i);
                node.getParentList().remove(root);
                root.getChildList().remove(node);
                i--;
            }
            taskList.remove(root);
        }
    }

}
