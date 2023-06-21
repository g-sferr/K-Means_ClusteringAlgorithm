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
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private int configurationDimension;
  private final Point point = new Point();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException
  {
      Configuration conf = context.getConfiguration();
      Path centersPath = new Path(conf.get("centroidsFilename"));
      SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centersPath));
      IntWritable key = new IntWritable();
      Centroid value = new Centroid();
      configurationDimension = Integer.parseInt(conf.get("dimension"));
      
      while (reader.next(key, value))
      {
          Centroid c = new Centroid(key, value.getCoordinates());

          centroids.add(c);
      }

      reader.close();
  }

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException
  {
    StringTokenizer itr = new StringTokenizer(value.toString(), ",");

    /*FRA: Adesso possiamo utilizzare il costruttore Point(String, int)*/

    List<DoubleWritable> coordinatesList = new ArrayList<DoubleWritable>();

    while (itr.hasMoreTokens() && (coordinatesList.size() < configurationDimension))
    {
      word.set(itr.nextToken());
      Double coordinate = Double.valueOf(word.toString());
      coordinatesList.add(new DoubleWritable(coordinate));
    }

    point.setCoordinates(coordinatesList);
    Centroid closestCentroid = null;
    Double minimumDistance = Double.MAX_VALUE;
    /*
    Double distancefromFirt = centroids.get(0).findEuclideanDistance(point);
    for(Centroid c : centroids.subList(1, centroids.size()))
    {
        Double distance = c.findEuclideanDistance(point);
        if(distance < distancefromFirt)
        {
            distancefromFirt = distance;
            closestCentroid = Centroid.copy(c);
        }
    }*/

    for (Centroid c1 : centroids) // Trovo il Centroide più vicino al puntoPassato al mapper
    {
      Double distance = c1.findEuclideanDistance(point);

      if (distance < minimumDistance)
      {
        minimumDistance = distance;
        closestCentroid = Centroid.copy(c1);
      }
    }

    context.write(closestCentroid, point);
  }

}