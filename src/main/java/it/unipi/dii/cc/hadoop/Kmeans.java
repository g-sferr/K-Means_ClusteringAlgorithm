package it.unipi.dii.cc.hadoop;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.ArrayList;
import java.util.List;

public class Kmeans
{
  private static int convergedCentroids = 0;

  /**
   * Verify stop condition, in particular return true if:
   *  - iterations are greater or equal to MAX_ITER
   *  - the euclidean distance between oldCentroids and newCentroids is less or equal to EPS
   *  Also count the number of converged centroids
   */
  private static boolean checkConditions(List <Centroid> newCentroids, List <Centroid> oldCentroids,
                                        int K, double EPS, int MAX_ITER, int iterations)
  {
    convergedCentroids = 0;

    // check if iterations are reached the maximum number
    if (iterations >= MAX_ITER) return true;

    // For each cluster, it checks if the distance between the old and new centroids is grater than EPS
    for (int i = 0; i < K; i++)
    {
      if(newCentroids.get(i).findEuclideanDistance(oldCentroids.get(i)) > EPS)
        return false;
      else
        convergedCentroids++;
    }

    // If in all coordinates the variation is below the epsilon, then it's time to stop
    return true;
  }

  /**
   * Retrieve from the iteration files the centroids returned by reducer in previous iteration
   */
  private static List<Centroid> retrieveResults(String OUT_FILE,
                                                Configuration conf) throws IOException
  {
    List<Centroid> toReturn = new ArrayList<>();
    FileSystem hdfs = FileSystem.get(conf);

    // status contains a list with all files in the /iteration-X folder
    FileStatus[] status = hdfs.listStatus(new Path(OUT_FILE));
    String line;

    // Check _SUCCESS
    if (!status[0].getPath().getName().startsWith("_SUCCESS"))
    {
      System.err.println("Error occurred at recovery partial files");
      System.exit(1);
    }

    // it starts from the second file because the first tells if it has succeeded or failed
    for (int i = 1; i < status.length; i++)
    {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath()))))
      {
        while ((line = reader.readLine()) != null)
        {
          String[] splittedCentroid = line.split("\t");
          toReturn.add( new Centroid(splittedCentroid[1],
                                      Integer.parseInt(Config.DIMENSIONS),
                                      Integer.parseInt(splittedCentroid[0])) );
        }
      }
    }
    return toReturn;
  }

  /**
   * Methods to write centroids (ID tab Coordinates) into output file
   */
  private static void writeCentroids(Configuration conf, List <Centroid> centroids,
                                          String output) throws IOException
  {
    FileSystem hdfs = FileSystem.get(conf);
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(output), true)));

    // Write centroid as: [id tab coordinate newLine]
    for(Centroid c : centroids)
    {
      br.write(c.getId().get() + "\t" + (new Point(c.getCoordinates())));
      br.newLine();
    }

    br.close();
    hdfs.close();
  }

  /**
   * Methods to write execution info (printInfo) into file (outputFile)
   */
  private static void writeInfo(Configuration conf, String[] printInfo, String outputFile) throws IOException
  {
    FileSystem hdfs = FileSystem.get(conf);
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(outputFile), true)));

    for(String s : printInfo)
    {
      br.write(s);
      br.newLine();
    }

    br.close();
    hdfs.close();
  }

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    List<Centroid> newCentroids;

    if (otherArgs.length < 2)
    {
      System.out.println("=======================");
      System.err.println("Usage: kmeans <input_file_name> <output_folder_name>");
      System.out.println("=======================");
      System.exit(2);
    }

    // Parameters taken from configuration file and command line
    System.out.println("\n\n=======================");
    System.out.println("args[0]: <input_file> = " + otherArgs[0]);
    System.out.println("=======================");
    System.out.println("args[1]: <output_folder> = " + otherArgs[1]);
    System.out.println("=======================");
    System.out.println("K: " + Config.K);
    System.out.println("=======================");
    System.out.println("Dimension: " + Config.DIMENSIONS);
    System.out.println("=======================");
    System.out.println("Threshold: " + Config.THRESHOLD);
    System.out.println("=======================\n\n");

    long start = System.currentTimeMillis();
    String OUTPUT_FILE = otherArgs[1];

    // Generate initial k random centroids
    newCentroids = Centroid.randomCentroidGenerator(otherArgs[0], Config.K, Config.DIMENSIONS, conf);
    //newCentroids = Centroid.staticCentroidLoader ("static_centroids.txt", Config.DIMENSIONS, conf);

    Path output = new Path(otherArgs[1]);
    FileSystem fs = FileSystem.get(output.toUri(), conf);

    // Delete output folder
    if (fs.exists(output))
    {
      System.out.println("Delete old output folder: " + output);
      fs.delete(output, true);
    }

    // Writes initial centroids into a file
    writeCentroids(conf, newCentroids, OUTPUT_FILE+"/initial_rand_Centroids.txt");

    System.out.println("\n\n\n===============================================");
    System.out.println("\n    ***** K-MEANS ALGORITHM STARTED *****\n");
    System.out.println("===============================================\n\n");

    boolean stop = false;
    boolean succeded; // Boolean value to check status of execution
    int iterations = 0;

    String iterationOutputPath;

    conf.set("k", Config.K);
    conf.set("threshold", Config.THRESHOLD);
    conf.set("dimension", Config.DIMENSIONS);

    while (!stop)
    {
      List<Centroid> oldCentroids = new ArrayList<>();

      iterations++;
      iterationOutputPath = OUTPUT_FILE + "/iteration-" + iterations;

      // Pass centroids to mapper
      newCentroids.forEach(c -> conf.set("centroid_" + c.getId().toString(), c.toString()));

      System.out.println("\n=======================");
      System.out.println("    ITERATION:    " + (iterations));
      System.out.println("    CONVERGED CENTROIDS:  " + convergedCentroids + "/" + Config.K);
	  System.out.println("=======================");
	  System.out.println("    CURRENT CENTROIDS:   " + newCentroids);
      System.out.println("=======================\n");

      Job job = Job.getInstance(conf, "Kmeans Job " + (iterations));
      job.setJarByClass(Kmeans.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      // set mapper/reducer
      job.setMapperClass(KMeansMapper.class);
      job.setCombinerClass(KMeansReducer.class);
      job.setReducerClass(KMeansReducer.class);

      int K = Integer.parseInt(Config.K); // k parameter from configuration file
      job.setNumReduceTasks(K); // set the number of reducer to k

      // define reducer's output key-value
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Point.class);

      // define I/O
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(iterationOutputPath));

      succeded = job.waitForCompletion(true);
      if(!succeded) // check if job finished successfully or not
      {
        System.err.println("Error at iteration "+iterations);
        System.exit(2);
      }

      // move newCentroids to oldCentroids
      newCentroids.forEach(c ->  oldCentroids.add(c.copy()));

      // retrieve new Centers from the previous iteration result file
      newCentroids = retrieveResults(iterationOutputPath, conf);

      stop = checkConditions(newCentroids, oldCentroids, K,
                            Double.parseDouble(Config.THRESHOLD),
                            Integer.parseInt(Config.MAX_ITER), iterations);

    }
    // Write final centroids into final_Centroids.txt
    writeCentroids(conf, newCentroids, OUTPUT_FILE+"/final_Centroids.txt");

    long end = System.currentTimeMillis();
    long elapsedTime = end - start;
    long minutes = (elapsedTime / 1000) / 60;
    long seconds = (elapsedTime / 1000) % 60;

    // Write final information into info_results.txt
    String[] infos = {
            "\n --- With respect to the following parameter configuration:",
            "\n=======================",
            "Cluster (K): " + Config.K,
            "Dimension: " + Config.DIMENSIONS,
            "Threshold: " + Config.THRESHOLD,
            "Max_Iterations: " + Config.MAX_ITER,
            "=======================",
            "\n1)  Total Execution Time: " + minutes + " min " + seconds + " sec",
            "2)  Total Iterations: " + iterations,
            "3)  Number of Converged Centroids: " + convergedCentroids + "\n"
    };
    writeInfo(conf, infos, OUTPUT_FILE+"/info_results.txt");

    // print final information
    System.out.println("\n=======================");
    System.out.println("::TOTAL EXECUTION TIME:: " + minutes + " min " + seconds + " sec");
    System.out.println("=======================");
    System.out.println("::FINAL CENTROIDS::   " + newCentroids);
    System.out.println("=======================");
    System.out.println("::TOTAL ITERATIONS:: " + iterations);
    System.out.println("=======================");
    System.out.println("::NUMBER OF CONVERGED CENTROIDS:: " + convergedCentroids);
    System.out.println("=======================\n");
  }
}
