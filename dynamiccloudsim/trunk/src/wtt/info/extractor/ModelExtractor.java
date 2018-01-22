package wtt.info.extractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.examples.Parameters.Distribution;
import wtt.info.WorkflowInfo;

public class ModelExtractor {
	
	public static void extracteWorkflowInfo() throws IOException{
		File file = new File("./dynamiccloudsim/model/modelInfo-workflow.txt");
		BufferedReader in = new BufferedReader(new FileReader(file));
		String line;
		int[] dim = new int[2];
		
		// workflowArrival
		line = in.readLine();
		String[] para_string = line.split("\t");
		dim[0] = Integer.parseInt(para_string[0]);
		Parameters.workflowArrival = new HashMap<>();
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			WorkflowInfo workflowInfo = new WorkflowInfo();
			line = in.readLine();
			para_string = line.split("\t");
			double timeslot = Double.parseDouble(para_string[0]);
			workflowInfo.submittedDCindex = Integer.parseInt(para_string[1]);
			double listlength = Integer.parseInt(para_string[2]);
//			List<String> workflowFileName = new ArrayList<>();
			for(int listindex = 0; listindex < listlength; listindex++) {
				line = in.readLine();
				workflowInfo.workflowFileName.add(line);
			}
			Parameters.workflowArrival.put(timeslot, workflowInfo);
		}
		in.close();
	}
	

	public static void extracteDCInfo() throws IOException {
		File file = new File("./dynamiccloudsim/model/modelInfo-dc.txt");
		BufferedReader in = new BufferedReader(new FileReader(file));
		String line;
		int[] dim = new int[2];
		
		
		
		
		
		// delayAmongDCIndex
		
		line = in.readLine();
		String[] para_string = line.split("\t");
		dim[0] = Integer.parseInt(para_string[0]);
		dim[1] = Integer.parseInt(para_string[1]);
		Parameters.delayAmongDCIndex = new float[dim[0]][dim[1]];
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			line = in.readLine();
			para_string = line.split("\t");
			for(int columnindex = 0; columnindex < dim[1]; columnindex++) {
				Parameters.delayAmongDCIndex[rowindex][columnindex] = (float) Double.parseDouble(para_string[columnindex]);
			}
		}
		
		// nOpteronOfMachineTypeOfDC
		line = in.readLine();
		para_string = line.split("\t");
		dim[0] = Integer.parseInt(para_string[0]);
		dim[1] = Integer.parseInt(para_string[1]);
		Parameters.nOpteronOfMachineTypeOfDC = new int[dim[0]][dim[1]];
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			line = in.readLine();
			para_string = line.split("\t");
			for(int columnindex = 0; columnindex < dim[1]; columnindex++) {
				Parameters.nOpteronOfMachineTypeOfDC[rowindex][columnindex] = Integer.parseInt(para_string[columnindex]);
			}
		}
		
		line = in.readLine();
		para_string = line.split("\t");
		dim[0] = Integer.parseInt(para_string[0]);
		
		
		int numberOfDC = dim[0];
		Parameters.degreeNumberOfDC = new int[dim[0]];
		
		Parameters.cpuHeterogeneityDistributionOfDC = new Distribution[numberOfDC];
		Parameters.cpuHeterogeneityCVOfDC = new double[numberOfDC];
		Parameters.ioHeterogeneityDistributionOfDC = new Distribution[numberOfDC];
		Parameters.ioHeterogeneityCVOfDC = new double[numberOfDC];
		Parameters.bwHeterogeneityDistributionOfDC = new Distribution[numberOfDC];
		Parameters.bwHeterogeneityCVOfDC = new double[numberOfDC];
		Parameters.cpuDynamicsDistributionOfDC = new Distribution[numberOfDC];
		Parameters.cpuDynamicsCVOfDC = new double[numberOfDC];
		Parameters.cpuBaselineChangesPerHourOfDC = new double[numberOfDC];
		Parameters.ioDynamicsDistributionOfDC = new Distribution[numberOfDC];
		Parameters.ioDynamicsCVOfDC = new double[numberOfDC];
		Parameters.ioBaselineChangesPerHourOfDC = new double[numberOfDC];
		Parameters.bwDynamicsDistributionOfDC = new Distribution[numberOfDC];
		Parameters.bwDynamicsCVOfDC = new double[numberOfDC];
		Parameters.bwBaselineChangesPerHourOfDC = new double[numberOfDC];
		Parameters.cpuNoiseDistributionOfDC = new Distribution[numberOfDC];
		Parameters.cpuNoiseCVOfDC = new double[numberOfDC];
		Parameters.ioNoiseDistributionOfDC = new Distribution[numberOfDC];
		Parameters.ioNoiseCVOfDC = new double[numberOfDC];
		Parameters.bwNoiseDistributionOfDC = new Distribution[numberOfDC];
		Parameters.bwNoiseCVOfDC = new double[numberOfDC];
		Parameters.likelihoodOfStragglerOfDC = new double[numberOfDC];
		Parameters.stragglerPerformanceCoefficientOfDC = new double[numberOfDC];
		Parameters.likelihoodOfFailure = new double[numberOfDC];
		Parameters.runtimeFactorInCaseOfFailure = new double[numberOfDC];
		Parameters.likelihoodOfDCFailure = new double[numberOfDC];
		Parameters.uplinkOfDC = new double[numberOfDC];
		Parameters.downlinkOfDC = new double[numberOfDC];
		Parameters.numberOfVMperDC = new int[numberOfDC];
		Parameters.MIPSbaselineOfDC = new double[numberOfDC];
		Parameters.bwBaselineOfDC = new double[numberOfDC];
		Parameters.ioBaselineOfDC = new double[numberOfDC];
		Parameters.ubOfDCFailureDuration = new double[numberOfDC];
		Parameters.lbOfDCFailureDuration = new double[numberOfDC];
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.degreeNumberOfDC[rowindex] = Integer.parseInt(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.cpuHeterogeneityCVOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.ioHeterogeneityCVOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.bwHeterogeneityCVOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.cpuDynamicsCVOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.cpuBaselineChangesPerHourOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.ioDynamicsCVOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.ioBaselineChangesPerHourOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.bwDynamicsCVOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.bwBaselineChangesPerHourOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.cpuNoiseCVOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.ioNoiseCVOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.bwNoiseCVOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.likelihoodOfStragglerOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.stragglerPerformanceCoefficientOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.likelihoodOfFailure[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.runtimeFactorInCaseOfFailure[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.likelihoodOfDCFailure[rowindex] =  Double.parseDouble(para_string[rowindex]);
			//Parameters.likelihoodOfDCFailure[rowindex] = 0d;
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.uplinkOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.downlinkOfDC[rowindex] =  Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.numberOfVMperDC[rowindex] = Integer.parseInt(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.ubOfDCFailureDuration[rowindex] = Double.parseDouble(para_string[rowindex]);
		}
		line = in.readLine();
		para_string = line.split("\t");
		for(int rowindex = 0; rowindex < dim[0]; rowindex++) {
			Parameters.lbOfDCFailureDuration[rowindex] = Double.parseDouble(para_string[rowindex]);
		}
		in.close();
		
		for(int dcindex = 0; dcindex < dim[0]; dcindex++) {
			Parameters.cpuHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
			
			Parameters.ioHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
			
			Parameters.bwHeterogeneityDistributionOfDC[dcindex] = Distribution.NORMAL;
			
			Parameters.cpuDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
			
			
			Parameters.ioDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
			
			Parameters.bwDynamicsDistributionOfDC[dcindex] = Distribution.NORMAL;
			
			Parameters.cpuNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
			
			Parameters.ioNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
			
			Parameters.bwNoiseDistributionOfDC[dcindex] = Distribution.NORMAL;
		}

	}

}
