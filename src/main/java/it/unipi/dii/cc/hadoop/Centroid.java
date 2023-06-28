package it.unipi.dii.cc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Centroid extends Point
{
  private IntWritable id;

  /*
  Methods override from Points class
 */
  @Override
  public void write(DataOutput out) throws IOException
  {
    super.write(out);
    out.writeInt(this.getId().get());
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    super.readFields(in);
    this.id = new IntWritable(in.readInt());
  }

  @Override
  public String toString() { return this.getId().get() + ";" + super.toString();}

  /*
  Constructors
  */

  Centroid() {
    super();

    this.id = new IntWritable(-1);
  }

  Centroid(int n)
  {
    super(n);
    this.id = new IntWritable(-1);
  }
  Centroid(IntWritable id, List<DoubleWritable> coordinates)
  {
    super(coordinates);
    this.id = new IntWritable(id.get());
  }
  Centroid(String coordinates, int configurationDimension, int ID)
  {
    super(coordinates, configurationDimension);
    this.id = new IntWritable(ID);
  }

  /*
  Get and Set Methods
  */
  public IntWritable getId(){ return this.id; }

  public void setId(IntWritable newId)
  {
    this.id = new IntWritable(newId.get());
  }


 /**
  * Methods that returns a new centroid with the same id and coordinates
  */
  public Centroid copy() { return new Centroid(this.getId(), this.getCoordinates()); }

  /**
   * Methods that returns the euclidean distance between the Centroid and the Point passed as argument
   */
  public Double findEuclideanDistance(Point point)
  {
    List<DoubleWritable> pointCoordinates = point.getCoordinates();
    double sum = 0.0;

    for (int i = 0; i < point.getCoordinates().size(); i++)
    {
        sum += Math.pow(this.getCoordinates().get(i).get() - pointCoordinates.get(i).get(), 2);
    }
    return Math.sqrt(sum);
  }

  /**
   * Methods that set coordinates of the Centroid as the sum of the coordinates of it and the
   * Point passed as argument
   */
  public void add(Point currentPoint)
  {
    int length = currentPoint.getCoordinates().size();
    List<DoubleWritable> currentPointCoordinates = currentPoint.getCoordinates();

    for(int i = 0; i < length; i++)
    {
      Double centroidCoordinate = this.getCoordinates().get(i).get();
      Double currentPointCoordinate = currentPointCoordinates.get(i).get();

      double sum = centroidCoordinate + currentPointCoordinate;
      this.getCoordinates().set(i, new DoubleWritable(sum));       
    }
  }

  /**
   * Methods that update coordinates of the Centroid calculate as the coordinates
   * divided by the number of points belong to its cluster
   */
  public void calculateMean(long numberPoints)
  {
    int length = this.getCoordinates().size();
    
    for(int i = 0; i < length; i++)
    {
      double centroidCoordinate = this.getCoordinates().get(i).get();
      double mean = centroidCoordinate / numberPoints;

      // Approximation of mean value to six decimal digit
      mean = (double) Math.round(mean * 1000000d) / 1000000d;
      this.getCoordinates().set(i, new DoubleWritable(mean));
    }
  }

  /**
   * Methods that returns a List of k (passed as argument) Centroids randomly generated
   * from the file passed as argument
   */
  public static List<Centroid> randomCentroidGenerator( String INPUT_FILE,
                                                        String k, String DIM,
                                                        Configuration conf)
                                                        throws IOException
  {
    final int numCentroid = Integer.parseInt(k);
    final int dimension = Integer.parseInt(DIM);
    final List<Centroid> randomCentroidsList = new ArrayList<>();

    Random random = new Random();
    List<Integer> indexRandomCentroid = new ArrayList<>();

    int pick;
    int dataSetSize = getLineNumber(INPUT_FILE, conf);

    // Generates k random numbers (that represents the index of lines of INPUT_FILE)
    while(indexRandomCentroid.size() < numCentroid)
    {
      pick = random.nextInt(dataSetSize) + 1;
      if(!indexRandomCentroid.contains(pick))
        indexRandomCentroid.add(pick);
    }

    Path path = new Path(INPUT_FILE);
    FileSystem hdfs = FileSystem.get(conf);

    int currentLine = 0;
    int ID = 0;
    String line;

    //Takes the corrective centroid to the randomly generated line and add it to the list
    try (BufferedReader reader = new
            BufferedReader(new InputStreamReader(hdfs.open(path))))
    {
      while(((line = reader.readLine()) != null) &&
              (randomCentroidsList.size() < numCentroid) )
      {
        if (indexRandomCentroid.contains(currentLine))
        {
          randomCentroidsList.add(new Centroid(line, dimension, ID));
          ID++;
        }
        currentLine++;
      }
    }
    return randomCentroidsList;
  }

  /**
   * Methods that returns the number of lines of input file passed as argument
   */
  private static int getLineNumber(String INPUT_FILE, Configuration conf) throws IOException
  {
    FileSystem fs = FileSystem.get(conf);
    int count = 0;

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(INPUT_FILE)))))
    {
      while (reader.readLine() != null)
      {
        count++;
      }
    }
    fs.close();

    return count;
  }
}