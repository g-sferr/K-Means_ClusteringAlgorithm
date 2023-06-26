package it.unipi.dii.cc.hadoop;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;


public class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Point>
{
    private static int dimension;
    private Text result = new Text("");
    private static double threshold;
    private final List<Centroid> centroids = new ArrayList<Centroid>();
    public static enum Counter {
      CONVERGED_COUNT
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        dimension = Integer.parseInt(conf.get("dimension"));
        threshold = Double.parseDouble(conf.get("threshold"));
    }

    @Override
    public void reduce(IntWritable key, Iterable<Point> values,
                       Context context) throws IOException, InterruptedException
    {
      Centroid meanCentroid = new Centroid(dimension);
      long numElements = 0;

      for (Point currentPoint : values)
      {
        meanCentroid.add(currentPoint);
        numElements++;
      }

      meanCentroid.setId(key);
      meanCentroid.calculateMean(numElements);

      //Double distance = key.findEuclideanDistance((Point) meanCentroid)
      Point point = new Point(meanCentroid.getCoordinates());
      //result.set(point);
      context.write(key, point);

      /*
      if (distance <= threshold)
      {
        context.getCounter(Counter.CONVERGED_COUNT).increment(1);
      }
       */
    }
  }