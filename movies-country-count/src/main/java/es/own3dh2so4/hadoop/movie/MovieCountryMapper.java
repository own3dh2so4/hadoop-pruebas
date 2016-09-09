package es.own3dh2so4.hadoop.movie;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieCountryMapper extends Mapper<LongWritable, Text, Text, Text>{

	enum Movie {
		MALFORMED_COUNTRY, MALFORMED_DATA
	}
	
	private MovieParser parser = new MovieParser();
	
	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		
		//skip first line with headers
		if (key.get() == 0) {
			return;
		}
		parser.parse(value);
		if (!parser.isValidCountry()) {
			System.err.println("Data error key: " + key.toString());
			System.err.println("Ignoring possibly corrupt country input data: " + parser.getMovieCountry());
		    context.getCounter(Movie.MALFORMED_COUNTRY).increment(1);
		    return;
		}
		
		if (!parser.isValidMovie()) {
			System.err.println("Data error key: " + key.toString());
			System.err.println("Expected data size: " + MovieParser.MOVIE_DATA_LENGTH);
			System.err.println("Real data size: " + parser.getNumberOfData());
			System.err.println("Ignoring possibly corrupt input: " + value );
		    context.getCounter(Movie.MALFORMED_DATA).increment(1);
		    return;
		}
		
		context.write(new Text(parser.getMovieCountry()), new Text(parser.getMovieTitle()));
	}
}
