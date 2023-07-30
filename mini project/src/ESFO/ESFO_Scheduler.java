package ESFO;


import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;

import ESFO.FitnessFunction;
import utils.Constants;
import utils.DatacenterCreator;
import utils.GenerateMatrices;

import java.text.DecimalFormat;
import java.util.*;

public class ESFO_Scheduler {

    private static List<Cloudlet> cloudletList;
    private static List<Vm> vmList;
    private static Datacenter[] datacenter;
    private static ESFO ESFOSchedularInstance;
    private static double mapping[];
    private static double[][] commMatrix;
    private static double[][] execMatrix;
    private static FitnessFunction ff = new FitnessFunction();
    private static List<Vm> createVM(int userId, int vms) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<Vm> list = new LinkedList<Vm>();

        //VM Parameters
        int vmsPerDC=vms/Constants.NO_OF_DATA_CENTERS;
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips = 500;//250
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        Vm[] vm = new Vm[vms];

        for (int i = 0; i < vms; i++) {
        	int dcIndex=i%Constants.NO_OF_DATA_CENTERS;
        	int dcId= datacenter[dcIndex].getId();
            vm[i] = new Vm(dcId,userId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }

        return list;
        
    }

    private static List<Cloudlet> createCloudlet(int userId, int cloudlets, int idShift) {
        LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

        //cloudlet parameters
        long fileSize = 300;
        long outputSize = 300;
        int pesNumber = 1;
        UtilizationModel utilizationModel = new UtilizationModelFull();

        Cloudlet[] cloudlet = new Cloudlet[cloudlets];

        for (int i = 0; i < cloudlets; i++) {
       
            int dcId = (int) (mapping[i]);
            long length = (long) (1e3 * (commMatrix[i][dcId] + execMatrix[i][dcId]));
            cloudlet[i] = new Cloudlet(idShift + i, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
            cloudlet[i].setUserId(userId);
            list.add(cloudlet[i]);
        }

        return list;
    }

    public static void main(String[] args) {
        Log.printLine("Starting ESFO Scheduler...");
        double[][] processingTimes={{1,9,8},{1,6,2},{3,4,2},{8,7,6},{8,7,10}};
        //double[][] processingTimes={{1,9,8},{1,6,2},{3,4,2},{8,7,6},{8,7,10},
        //{1,9,8},{1,6,2},{3,4,2},{8,7,6},{8,7,10}};
        new GenerateMatrices();
        commMatrix = GenerateMatrices.getCommMatrix();
        execMatrix = GenerateMatrices.getExecMatrix();
        ESFOSchedularInstance = new ESFO(Constants.NO_OF_TASKS,Constants.NO_OF_DATA_CENTERS,100,0.5,10,processingTimes);
        mapping=ESFOSchedularInstance.run();
       
        try {
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            CloudSim.init(num_user, calendar, trace_flag);

            // Second step: Create Datacenters
            datacenter = new Datacenter[Constants.NO_OF_DATA_CENTERS];
            for (int i = 0; i < Constants.NO_OF_DATA_CENTERS; i++) {
                datacenter[i] = DatacenterCreator.createDatacenter("Datacenter_" + i);
            }

            //Third step: Create Broker
            ESFODatacenterBroker broker = createBroker("Broker_0");
            int brokerId = broker.getId();

            //Fourth step: Create VMs and Cloudlets and send them to broker
            //vmList = createVM(brokerId,5);
            vmList = createVM(brokerId,10);
            cloudletList = createCloudlet(brokerId, Constants.NO_OF_TASKS, 0);

            // mapping our dcIds to cloudsim dcIds
            HashSet<Integer> dcIds = new HashSet<>();
            HashMap<Integer, Integer> hm = new HashMap<>();
            for (Datacenter dc : datacenter) {
                if (!dcIds.contains(dc.getId()))
                    dcIds.add(dc.getId());
            }
            Iterator<Integer> it = dcIds.iterator();
            for (int i = 0; i < mapping.length; i++) {
                if (hm.containsKey((int) mapping[i])) continue;
                hm.put((int) mapping[i], it.next());
            }
            for (int i = 0; i < mapping.length; i++)
                mapping[i] = hm.containsKey((int) mapping[i]) ? hm.get((int) mapping[i]) : mapping[i];

            broker.submitVmList(vmList);
            broker.setMapping(mapping);
            broker.submitCloudletList(cloudletList);


            // Fifth step: Starts the simulation
            CloudSim.startSimulation();

            List<Cloudlet> newList = broker.getCloudletReceivedList();

            CloudSim.stopSimulation();

            printCloudletList(newList);

            Log.printLine(ESFO_Scheduler.class.getName() + " finished!");
            ESFOSchedularInstance.getBestSchedule();
            System.out.println("Makespan:"+ff.calcMakespan(mapping));
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }

    private static ESFODatacenterBroker createBroker(String name) throws Exception {
        return new ESFODatacenterBroker(name);
    }

    /**
     * Prints the Cloudlet objects
     *
     * @param list list of Cloudlets
     */
    private static void printCloudletList(List<Cloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" +
                indent + "Data center ID" +
                indent + "VM ID" +
                indent + indent + "Time" +
                indent + "Start Time" +
                indent + "Finish Time");

        double mxFinishTime = 0;
        DecimalFormat dft = new DecimalFormat("###.##");
        dft.setMinimumIntegerDigits(2);
        for (int i = 0; i < size; i++) {
            cloudlet = list.get(i);
            Log.print(indent + dft.format(cloudlet.getCloudletId()) + indent + indent);

            if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");
                Log.printLine(indent + indent + dft.format(cloudlet.getResourceId()) +
                        indent + indent + indent + dft.format(cloudlet.getVmId()) +
                        indent + indent + dft.format(cloudlet.getActualCPUTime()) +
                        indent + indent + dft.format(cloudlet.getExecStartTime()) +
                        indent + indent + indent + dft.format(cloudlet.getFinishTime()));
            }
            mxFinishTime = Math.max(mxFinishTime, cloudlet.getFinishTime());
        }
        Log.printLine(mxFinishTime);

    }
}