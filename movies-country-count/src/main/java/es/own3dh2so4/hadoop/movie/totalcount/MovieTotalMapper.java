package es.own3dh2so4.hadoop.movie.totalcount;

import es.own3dh2so4.hadoop.movie.MovieParser;
import es.own3dh2so4.hadoop.movie.countrycount.MovieCountryMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by david on 18/09/16.
 */
public class MovieTotalMapper extends Mapper<LongWritable, Text, NullWritable, Text> {


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
            context.getCounter(MovieTotalMapper.Movie.MALFORMED_COUNTRY).increment(1);
            return;
        }

        if (!parser.isValidMovie()) {
            System.err.println("Data error key: " + key.toString());
            System.err.println("Expected data size: " + MovieParser.MOVIE_DATA_LENGTH);
            System.err.println("Real data size: " + parser.getNumberOfData());
            System.err.println("Ignoring possibly corrupt input: " + value );
            context.getCounter(MovieTotalMapper.Movie.MALFORMED_DATA).increment(1);
            return;
        }

        context.write(NullWritable.get(), new Text(parser.getMovieTitle()));
    }
}
