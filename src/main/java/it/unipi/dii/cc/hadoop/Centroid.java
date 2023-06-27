package it.unipi.dii.cc.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.hadoop.io.SequenceFile;

public class Centroid extends Point
{
  private IntWritable id; // FRA: Attributo da lasciare?

  Centroid()
  {
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

  public IntWritable getId(){ return this.id; }

  public void setId(IntWritable newId)
  {
    this.id = new IntWritable(newId.get());
  }

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
  public String toString()
  {
    return this.getId().get() + ";" + super.toString();
  }

  // Make a deep copy of the centroid 
  public Centroid copy()
  {
    return new Centroid(this.getId(), this.getCoordinates());
  }

  public Double findEuclideanDistance(Point point)
  {
    int lenght = point.getCoordinates().size();
    List<DoubleWritable> pointCoordinates = point.getCoordinates();
    Double sum = 0.0;

    for (int i = 0; i < lenght; i++)
    {
        Double difference = this.getCoordinates().get(i).get()
                              - pointCoordinates.get(i).get();
        sum += Math.pow(difference, 2);
    }
    return Math.sqrt(sum);
  }
  
  public void add(Point currentPoint)
  {
    int length = currentPoint.getCoordinates().size();
    List<DoubleWritable> currentPointCoordinates = currentPoint.getCoordinates();

    for(int i = 0; i < length; i++)
    {
      Double centroidCoordinate = this.getCoordinates().get(i).get();
      Double currentPointCoordinate = currentPointCoordinates.get(i).get();

      Double sum = centroidCoordinate + currentPointCoordinate;
      this.getCoordinates().set(i, new DoubleWritable(sum));       
    }
  }

  public void calculateMean(long numElements)
  {
    int length = this.getCoordinates().size();
    
    for(int i = 0; i < length; i++)
    {
      Double centroidCoordinate = this.getCoordinates().get(i).get();
      Double mean = centroidCoordinate / numElements;

      // FRA: Frare prova con e senza questa approssimazione
      mean = (double) Math.round(mean * 1000000d) / 1000000d;
      this.getCoordinates().set(i, new DoubleWritable(mean));
    }
  }

  public static List<Centroid> randomCentroidGenerator( String INPUT_FILE,
                                                        String k, String DIM,
                                                        Configuration conf)
                                                        throws IOException
  {
    final int numCentroid = Integer.parseInt(k);
    final int dimension = Integer.parseInt(DIM);
    final List<Centroid> randomCentroidsList = new ArrayList<>();

    Random random = new Random();
    //Lista contenente 'numCentroid' numeri casuali univoci
    List<Integer> indexRandomCentroid = new ArrayList<>();

    int pick;
    int dataSetSize = getLineNumber(INPUT_FILE, conf);

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

    try (BufferedReader reader = new
            BufferedReader(new InputStreamReader(hdfs.open(path))))
    {
      while(((line = reader.readLine()) != null) &&
              (randomCentroidsList.size() < numCentroid) )
      {
        if (indexRandomCentroid.contains(currentLine))
        {
          Centroid c = new Centroid(line, dimension, ID);
          randomCentroidsList.add(c);
          ID++;
        }
        currentLine++;
      }
    }
    return randomCentroidsList;
  }

  private static int getLineNumber(String INPUT_FILE,
                                   Configuration conf) throws IOException
  {
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(INPUT_FILE);
    int count = 0;

    try (BufferedReader reader = new BufferedReader(new
                                    InputStreamReader(fs.open(path))))
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