package wtt.test.parsefile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.core.CloudSim;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.workflowsim.FileItem;
import org.workflowsim.utils.ReplicaCatalog;


import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.examples.Parameters.FileType;
import de.huberlin.wbi.dcs.workflow.Task;

public class ParseFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for(int index = 0; index < Parameters.workflow_relative_Candidate.length; index++) {
			parseXmlFile(Parameters.workflow_relative_Candidate[index]);
		}
	}
	
	private static void parseXmlFile(String path) {
    	List<Task> taskListInWorkflow  = new ArrayList<>();
        try {

            SAXBuilder builder = new SAXBuilder();
            //parse using builder to get DOM representation of the XML file
            Document dom = builder.build(new File(path));
            Element root = dom.getRootElement();
            List<Element> list = root.getChildren();
//            // each workflow has one submittedPos
            
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
                        if(length > 1e7) {
                        	int a = 0;
                        	a = a + 1;
                        }
                        
                        
        				
        				
        				
                        /**
                         * Add dependencies info.
                         */
                        break;
                    case "child":
                        List<Element> pList = node.getChildren();
                        String childName = node.getAttributeValue("ref");
                        
                        break;
                }
            }
            /**
             * If a task has no parent, then it is root task.
             */
            

            /**
             * Add depth from top to bottom.
             */
            
            /**
             * Clean them so as to save memory. Parsing workflow may take much
             * memory
             */
            

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
