package ESFO;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import ESFO.FitnessFunction;
import utils.Constants;
import utils.DatacenterCreator;
import utils.GenerateMatrices;

import java.text.DecimalFormat;
import java.util.*;


public class ESFO {

    // Parameters of the algorithm
    private int n; // Number of tasks
    private int m; // Number of processors
    private int k; // Number of sunflowers
    private double c; // Convergence factor
    private int maxIterations; // Maximum number of iterations
    private double[][] processingTimes; // Processing times for each task and processor
    private int[][] schedules; // Schedule of tasks on each processor
    private double[] fitnessValues; // Fitness values for each sunflower
    private double[] bestFitnessValues; // Best fitness values for each sunflower
    private int[][] bestSchedules; // Best schedules for each sunflower
    private Random random; // Random number generator
    private static FitnessFunction ff = new FitnessFunction();
    public ESFO(int n, int m, int k, double c, int maxIterations, double[][] processingTimes) {
        this.n = n;
        this.m = m;
        this.k = k;
        this.c = c;
        this.maxIterations = maxIterations;
        this.processingTimes = processingTimes;
        this.schedules = new int[m][n];
        this.fitnessValues = new double[k];
        this.bestFitnessValues = new double[k];
        this.bestSchedules = new int[k][m * n];
        this.random = new Random();
    }

    // Initialize the schedules randomly
    private void initializeSchedules() {
        for (int i = 0; i < m; i++)/*change to 0*/ {
            List<Integer> tasks = new ArrayList<>();
            for (int j = 0; j < n; j++)/*change to 0*/ {
                tasks.add(j);
            }
            Collections.shuffle(tasks, random);
            for (int j = 0; j < n; j++) {
                schedules[i][j] = tasks.get(j);
            }
        }
    }

    // Evaluate the fitness value of each sunflower
    private void evaluateFitness() {
        for (int i = 0; i < k; i++) {
            int[] schedule = bestSchedules[i];
            double fitness = 0.0;
            for (int j = 0; j < m; j++) {
                for (int l = 0; l < n; l++) {
                    int task = schedule[j * n + l];
                    //fitness += processingTimes[task][j] * (l + 1);
                   
                }
            }
            fitnessValues[i] = fitness;
            if (fitness < bestFitnessValues[i]) {
                bestFitnessValues[i] = fitness;
                System.arraycopy(schedule, 0, bestSchedules[i], 0, m * n);
            }
           
        }
    }

    // Update the positions of the sunflowers
    private void updatePositions() {
        for (int i = 0; i < k; i++) {
            int[] schedule = bestSchedules[i];
            double fitness = fitnessValues[i];
            for (int j = 0; j < m * n; j++) {
                double x = schedule[j];
                double r = random.nextDouble();
                double phi = random.nextDouble() * 2 * Math.PI;
                double s = random.nextDouble();
                double t = random.nextDouble();
                double delta = (s * Math.sin(phi) + t * Math.cos(phi)) * r;
                x = x + delta * (bestFitnessValues[i] - fitness) / (1 + c * Math.abs(delta));
                if (x < 0) {
                    x = 0;
                } else
                    if (x > n - 1) {
                        x = n - 1;
                    }
                    schedule[j] = (int) x;
                }
                System.arraycopy(schedule, 0, bestSchedules[i], 0, m * n);
            }
        }

        // Run the ESO algorithm
        public double[] run() {
            initializeSchedules();
            evaluateFitness();
            System.arraycopy(bestFitnessValues, 0, fitnessValues, 0, k);
            for (int i = 0; i < maxIterations; i++) {
                updatePositions();
                evaluateFitness();
                for (int j = 0; j < k; j++) {
                    if (fitnessValues[j] < bestFitnessValues[j]) {
                        bestFitnessValues[j] = fitnessValues[j];
                        System.arraycopy(bestSchedules[j], 0, schedules[j], 0, m * n);
                    }
                }
            }
            return bestFitnessValues;
        }

        // Get the best schedule
        void  getBestSchedule() {
            int[][] bestSchedule = new int[m][n];
            for (int i = 0; i < m; i++) {
                System.arraycopy(schedules[i], 0, bestSchedule[i], 0, n);
            }
            for(int i=0;i<schedules.length;i++)
            { System.out.print("VM:"+(i+1)+"---");
            for(int j=0;j<schedules[i].length;j++)
            {
            System.out.print(schedules[i][j]+" ");
            }
            System.out.print("\n");
           
            }
        }
        /*public static void main(String args[])
        {
        int n=5;
        int m=3;
        int k=100;
        double c=0.5;
        int maxIterations=10;
        double [][] processingTimes= {{1,9,8},{1,6,2},{3,4,2},{8,7,6},{8,7,10}};
        ESFO e=new ESFO(n,m,k,c,maxIterations,processingTimes);
        e.run();
        e.getBestSchedule();
       
        }*/
}
