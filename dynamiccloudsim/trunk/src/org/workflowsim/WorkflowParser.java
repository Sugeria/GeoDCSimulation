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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.analysis.function.Max;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import org.workflowsim.utils.ReplicaCatalog;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.examples.Parameters.FileType;
import de.huberlin.wbi.dcs.workflow.Task;

/**
 * WorkflowParser parse a DAX into tasks so that WorkflowSim can manage them
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Aug 23, 2013
 * @date Nov 9, 2014
 */
public final class WorkflowParser {

    /**
     * The path to DAX file.
     */
    public String daxPath;
    /**
     * The path to DAX files.
     */
    public List<String> daxPaths;
    /**
     * All tasks.
     */
    private List<Task> taskList;
    
    public HashMap<Integer, List<Task>> taskListOfWorkflow;
    /**
     * User id. used to create a new task.
     */
    private final int userId;
    public int workflowId = 0;

    /**
     * current job id. In case multiple workflow submission
     */
    private int jobIdStartsFrom;

    /**
     * Gets the task list
     *
     * @return the task list
     */
    @SuppressWarnings("unchecked")
    public List<Task> getTaskList() {
        return taskList;
    }

    /**
     * Sets the task list
     *
     * @param taskList the task list
     */
    protected void setTaskList(List<Task> taskList) {
        this.taskList = taskList;
    }
    /**
     * Map from task name to task.
     */
    protected Map<String, Task> mName2Task;

    /**
     * Initialize a WorkflowParser
     *
     * @param userId the user id. Currently we have just checked single user
     * mode
     */
    public WorkflowParser(int userId) {
        this.userId = userId;
        this.workflowId = 0;
        this.mName2Task = new HashMap<>();
        this.daxPath = Parameters.getDaxPath();
        this.daxPaths = Parameters.getDAXPaths();
        this.jobIdStartsFrom = 1;
        this.taskListOfWorkflow = new HashMap<>();
        setTaskList(new ArrayList<>());
    }

    /**
     * Start to parse a workflow which is a xml file(s).
     */
    public void parse() {
    	taskListOfWorkflow.clear();
        if (this.daxPath != null) {
        	workflowId++;
            parseXmlFile(this.daxPath,workflowId);
        } else if (this.daxPaths != null) {
            for (String path : this.daxPaths) {
            	workflowId++;
                parseXmlFile(path,workflowId);
            }
        }
    }

    /**
     * Sets the depth of a task
     *
     * @param task the task
     * @param depth the depth
     */
    private void setDepth(Task task, int depth) {
        if (depth > task.getDepth()) {
            task.setDepth(depth);
        }
        for (Task cTask : task.getChildList()) {
            setDepth(cTask, task.getDepth() + 1);
        }
    }

    /**
     * Parse a DAX file with jdom
     */
    private void parseXmlFile(String path,int workflowId) {
    	List<Task> taskListInWorkflow  = new ArrayList<>();
        try {

            SAXBuilder builder = new SAXBuilder();
            //parse using builder to get DOM representation of the XML file
            Document dom = builder.build(new File(path));
            Element root = dom.getRootElement();
            List<Element> list = root.getChildren();
            // each workflow has one submittedPos
            int submittedPos = ((int)((Math.random()*Parameters.numberOfDC)) % Parameters.numberOfDC);
            for (Element node : list) {
                switch (node.getName().toLowerCase()) {
                    case "job":
                        long length = 0;
                        String nodeName = node.getAttributeValue("id");
                        String nodeType = node.getAttributeValue("name");
                        /**
                         * capture runtime. If not exist, by default the runtime
                         * is 0.1. Otherwise CloudSim would ignore this task.
                         * BUG/#11
                         */
                        double runtime;
                        if (node.getAttributeValue("runtime") != null) {
                            String nodeTime = node.getAttributeValue("runtime");
                            runtime = 1000 * Double.parseDouble(nodeTime);
                            if (runtime < 100) {
                                runtime = 100;
                            }
                            length = (long) runtime;
                        } else {
                            Log.printLine("Cannot find runtime for " + nodeName + ",set it to be 0");
                        }   //multiple the scale, by default it is 1.0
                        length *= Parameters.getRuntimeScale();
                        List<Element> fileList = node.getChildren();
                        List<FileItem> mFileList = new ArrayList<>();
                        double inputSize = 0d;
                        double outputSize = 0d;
                        for (Element file : fileList) {
                            if (file.getName().toLowerCase().equals("uses")) {
                                String fileName = file.getAttributeValue("name");//DAX version 3.3
                                if (fileName == null) {
                                    fileName = file.getAttributeValue("file");//DAX version 3.0
                                }
                                if (fileName == null) {
                                    Log.print("Error in parsing xml");
                                }

                                String inout = file.getAttributeValue("link");
                                double size = 0.0;

                                String fileSize = file.getAttributeValue("size");
                                if (fileSize != null) {
                                    size = Double.parseDouble(fileSize) /*/ 1024*/;
                                    
                                } else {
                                    Log.printLine("File Size not found for " + fileName);
                                }

                                /**
                                 * a bug of cloudsim, size 0 causes a problem. 1
                                 * is ok.
                                 */
                                if (size == 0) {
                                    size++;
                                }
                                /**
                                 * Sets the file type 1 is input 2 is output
                                 */
                                FileType type = FileType.NONE;
                                switch (inout) {
                                    case "input":
                                        type = FileType.INPUT;
                                        break;
                                    case "output":
                                        type = FileType.OUTPUT;
                                        break;
                                    default:
                                        Log.printLine("Parsing Error");
                                        break;
                                }
                                FileItem tFile;
                                /*
                                 * Already exists an input file (forget output file)
                                 */
                                if (size < 0) {
                                    /*
                                     * Assuming it is a parsing error
                                     */
                                    size = 0 - size;
                                    Log.printLine("Size is negative, I assume it is a parser error");
                                }
                                /*
                                 * Note that CloudSim use size as MB, in this case we use it as Byte
                                 */
                                if (type == FileType.OUTPUT) {
                                    /**
                                     * It is good that CloudSim does tell
                                     * whether a size is zero
                                     */
                                    tFile = new FileItem(fileName, size);
                                    outputSize += size;
                                } else if (ReplicaCatalog.containsFile(fileName)) {
                                    tFile = ReplicaCatalog.getFile(fileName);
                                    inputSize += size;
                                } else {

                                    tFile = new FileItem(fileName, size);
                                    ReplicaCatalog.setFile(fileName, tFile);
                                    inputSize += size;
                                }

                                tFile.setType(type);
                                mFileList.add(tFile);

                            }
                        }
                        String params = "null";
                        
                        Task task;
                        
                        // int numberofData = (int)(Math.random()*Parameters.ubOfData);
                        int numberofData = Parameters.ubOfData;

                        int[] positionOfData = null;
        				int[] sizeOfData = null;
        				long remoteInputSize = 0;
        				
        				
//        				if (numberofData != 0) {
//        					
//        					positionOfData = new int[numberofData];
//        					
//        					sizeOfData = new int[numberofData];
//        					
//        					remoteInputSize = 0;
//        					
//        					for (int dataindex = 0; dataindex < numberofData; dataindex++) {
//        						positionOfData[dataindex] = ((int)((Math.random()*Parameters.numberOfDC)) % Parameters.numberOfDC);
//        						sizeOfData[dataindex] = (int)(Math.random()*(Parameters.ubOfDataSize - Parameters.lbOfDataSize ) + Parameters.lbOfDataSize);
//        						remoteInputSize += sizeOfData[dataindex];
//        					}
//        				}
        				
        				
        				long ioLength = (remoteInputSize + (long)inputSize + (long)outputSize) / 1024;
                        long bwLength = remoteInputSize / 1024;
                        int pesNumber = 1;
                        UtilizationModel utilizationModel = new UtilizationModelFull();
                        //In case of multiple workflow submission. Make sure the jobIdStartsFrom is consistent.
                        synchronized (this) {
                            task = new Task(nodeType,params,userId,this.jobIdStartsFrom,length,
                            		ioLength,bwLength,pesNumber,(long)inputSize,(long)outputSize,
                            		utilizationModel,utilizationModel,utilizationModel,submittedPos);
                            this.jobIdStartsFrom++;
                        }
//                      if (numberofData != 0) {
//        					task.numberOfData = numberofData;
//        					task.positionOfData = positionOfData;
//        					task.sizeOfData = sizeOfData;
//        					task.requiredBandwidth = new double[numberofData];
//        					task.positionOfDataID = new int[numberofData];
//        					task.numberOfTransferData = new int[Parameters.numberOfDC];
//        				}
                        task.setType(nodeType);
                        task.setUserId(userId);
                        task.workflowId = workflowId;
                        mName2Task.put(nodeName, task);
                        for (FileItem file : mFileList) {
                            task.addRequiredFile(file.getName());
                        }
                        task.setFileList(mFileList);
                        taskListInWorkflow.add(task);

                        /**
                         * Add dependencies info.
                         */
                        break;
                    case "child":
                        List<Element> pList = node.getChildren();
                        String childName = node.getAttributeValue("ref");
                        if (mName2Task.containsKey(childName)) {

                            Task childTask = (Task) mName2Task.get(childName);

                            for (Element parent : pList) {
                                String parentName = parent.getAttributeValue("ref");
                                if (mName2Task.containsKey(parentName)) {
                                    Task parentTask = (Task) mName2Task.get(parentName);
                                    parentTask.addChild(childTask);
                                    childTask.addParent(parentTask);
                                }
                            }
                        }
                        break;
                }
            }
            /**
             * If a task has no parent, then it is root task.
             */
            ArrayList roots = new ArrayList<>();
            for (Task task : mName2Task.values()) {
                task.setDepth(0);
                if (task.getParentList().isEmpty()) {
                    roots.add(task);
                }
            }

            /**
             * Add depth from top to bottom.
             */
            for (Iterator it = roots.iterator(); it.hasNext();) {
                Task task = (Task) it.next();
                setDepth(task, 1);
            }
            /**
             * Clean them so as to save memory. Parsing workflow may take much
             * memory
             */
            taskListOfWorkflow.put(workflowId, taskListInWorkflow);
            this.mName2Task.clear();

        } catch (JDOMException jde) {
            Log.printLine("JDOM Exception;Please make sure your dax file is valid");

        } catch (IOException ioe) {
            Log.printLine("IO Exception;Please make sure dax.path is correctly set in your config file");

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Parsing Exception");
        }
    }
}
