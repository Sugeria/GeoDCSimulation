package wtt.info.generator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import de.huberlin.wbi.dcs.examples.Parameters;
import wtt.info.WorkflowInfo;

public class ModelGenerator {
	
	public static void saveWorkflowInfo() throws IOException {
		File file = new File("./model/modelInfo-workflow.txt");
		FileWriter out = new FileWriter(file);
		// workflowArrival
		int length = Parameters.workflowArrival.size();
		int[] dim = new int[2];
		dim[0] = length;
		out.write(dim[0]+"\t");
		out.write("\r\n");
		for(Double key:Parameters.workflowArrival.keySet()) {
			out.write(key+"\t");
			WorkflowInfo workflowInfo = Parameters.workflowArrival.get(key);
			out.write(workflowInfo.submittedDCindex+"\t");
			int workflowNum = workflowInfo.workflowFileName.size();
			out.write(workflowNum+"\t");
			out.write("\r\n");
			for(int listindex = 0; listindex < workflowNum; listindex++) {
				out.write(workflowInfo.workflowFileName.get(listindex));
				out.write("\r\n");
			}
		}
		out.close();
	}

	public static void saveDCInfo() throws IOException {
		File file = new File("./model/modelInfo-dc.txt");
		FileWriter out = new FileWriter(file);
		// workflowArrival
		int[] dim = new int[2];
		
		// delayAmongDCIndex
		dim[0] = Parameters.numberOfDC;
		dim[1] = Parameters.numberOfDC;
		out.write(dim[0]+"\t");
		out.write(dim[1]+"\t");
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			for(int columnindex = 0; columnindex < dim[1]; columnindex++) {
				out.write(Parameters.delayAmongDCIndex[rowindex][columnindex]+"\t");
			}
			out.write("\r\n");
		}
		
		// nOpteronOfMachineTypeOfDC
		dim[1] = Parameters.machineType;
		out.write(dim[0]+"\t");
		out.write(dim[1]+"\t");
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			for(int columnindex = 0; columnindex < dim[1]; columnindex++) {
				out.write(Parameters.nOpteronOfMachineTypeOfDC[rowindex][columnindex]+"\t");
			}
			out.write("\r\n");
		}
		
		// degreeNumberOfDC 
		out.write(dim[0]+"\t");
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.degreeNumberOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		// datacenterinfo
		
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.cpuHeterogeneityCVOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.ioHeterogeneityCVOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.bwHeterogeneityCVOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.cpuDynamicsCVOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.cpuBaselineChangesPerHourOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.ioDynamicsCVOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.ioBaselineChangesPerHourOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.bwDynamicsCVOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.bwBaselineChangesPerHourOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.cpuNoiseCVOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.ioNoiseCVOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.bwNoiseCVOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.likelihoodOfStragglerOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.stragglerPerformanceCoefficientOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.likelihoodOfFailure[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.runtimeFactorInCaseOfFailure[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.likelihoodOfDCFailure[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.uplinkOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.downlinkOfDC[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.numberOfVMperDC[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.ubOfDCFailureDuration[rowindex]+"\t");
		}
		out.write("\r\n");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			out.write(Parameters.lbOfDCFailureDuration[rowindex]+"\t");
		}
		out.write("\r\n");
		out.close();
	}

}
