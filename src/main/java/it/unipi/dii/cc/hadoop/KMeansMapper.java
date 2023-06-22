package it.unipi.dii.cc.hadoop;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

public class KMeansMapper extends Mapper<Object, Text, Centroid, Point> {
  private final List<Centroid> centroids = new ArrayList<>();
  private Text word = new Text();
  private int configurationDimension;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException
  {
      Configuration conf = context.getConfiguration();

      //Path centersPath = new Path(conf.get("centroidsFilename"));
      //SequenceFile.Reader reader = new SequenceFile.Reader(conf SequenceFile.Reader.file(centersPath));
      IntWritable key = new IntWritable();
      Centroid value = new Centroid();
      configurationDimension = Integer.parseInt(conf.get("dimension"));
      int k = Integer.parseInt(conf.get("k"));

      for(int i = 0; i < k; i++) // es: 0;0.1,0.2
      {
          //Utilizzare String.split() al posto di itr???
          StringTokenizer itr = new StringTokenizer(context.getConfiguration().get("center_" + i), ";");
          String[] splittedCentroid = context.getConfiguration().get("center_" + i).split(";");
          centroids.add( new Centroid(splittedCentroid[1], configurationDimension, Integer.parseInt(splittedCentroid[0])) );
      }
  }

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException
  {
    Point point = new Point(value.toString(), configurationDimension);

    Centroid closestCentroid = null;
    Double minimumDistance = Double.MAX_VALUE;

    for (Centroid c1 : centroids) // Trovo il Centroide più vicino al puntoPassato al mapper
    {
      Double distance = c1.findEuclideanDistance(point);

      if (distance < minimumDistance)
      {
        minimumDistance = distance;
        closestCentroid = c1.copy();
      }
    }
    context.write(closestCentroid, point);
  }
}