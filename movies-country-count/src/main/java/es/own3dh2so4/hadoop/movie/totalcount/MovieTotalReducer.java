package es.own3dh2so4.hadoop.movie.totalcount;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by david on 18/09/16.
 */
public class MovieTotalReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

    @Override
    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {
        context.write(new Text("Films"), new IntWritable(Iterables.size(values)));
    }
}
