package es.own3dh2so4.hadoop.movie.countrycount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

public class MovieCountryReducer extends Reducer<Text, Text, Text, IntWritable> {

	
	@Override
	  public void reduce(Text key, Iterable<Text> values,
	      Context context)
	      throws IOException, InterruptedException {
		context.write(key, new IntWritable(Iterables.size(values)));
	}
}
