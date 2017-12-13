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
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;

import de.huberlin.wbi.dcs.workflow.Task;

/**
 * Job is an extention to Task. It is basically a group of tasks. In
 * WorkflowSim, the ClusteringEngine merges tasks into jobs (group of tasks) and
 * the overall runtime of a job is the sum of the task runtime.
 *
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class Job extends Task {

    /*
     * The list of tasks a job has. It is the only difference between Job and Task. 
     */
    private List<Task> taskList;
    private long length;
    public List<Task> successTaskList;

    /**
     * Allocates a new Job object. The job length should be greater than or
     * equal to 1.
     *
     * @param jobId the unique ID of this job
     * @param jobLength the length or size (in MI) of this task to be executed
     * in a PowerDatacenter
     * @pre jobId >= 0
     * @pre jobLength >= 0.0
     * @post $none
     */
    public Job(int userId,
            int jobId,
            long jobLength) {
        super("Job" + Integer.toString(jobId),"null",userId,jobId,1,0,0,1,0,0,new UtilizationModelFull(),new UtilizationModelFull(),new UtilizationModelFull());
        this.taskList = new ArrayList<>();
        this.length = jobLength;
        // when job retry remember to record the execution time and the already successful tasks
        this.successTaskList = new ArrayList<>();
    }


	/**
     * Gets the list of tasks in this job
     *
     * @return the list of the tasks
     * @pre $none
     * @post $none
     */
    public List<Task> getTaskList() {
        return this.taskList;
    }

    /**
     * Sets the list of the tasks
     *
     * @param list, list of the tasks
     */
    public void setTaskList(List list) {
        this.taskList = list;
    }

    /**
     * Adds a task list to the existing task list
     *
     * @param list, task list to be added
     */
    public void addTaskList(List list) {
        this.taskList.addAll(list);
    }

    /**
     * Gets the list of the parent tasks and override its super function
     *
     * @return the list of the parents
     * @pre $none
     * @post $none
     */
    @Override
    public List getParentList() {
        return super.getParentList();
    }
}
