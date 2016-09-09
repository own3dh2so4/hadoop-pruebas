package es.own3dh2so4.hadoop.movie;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class MovieParser {

	private String[] movieData;
	
	private final static int MOVIE_TITLE_POSITION=11;
	private final static int MOVIE_COUNTRY_POSITION=20;
	
	public final static int MOVIE_DATA_LENGTH = 28;
	
	public MovieParser() {
		movieData = new String[0];
	}
	
	public MovieParser(Text movie){
		parse(movie);
	}

	public void parse(Text movie) {
		String movieString = movie.toString();
		if (movieString.contains("\"")){
			List<String> data = new LinkedList<>();
			String[] splitedTitle = movieString.split("\"");
			data.addAll(Arrays.asList(splitedTitle[0].substring(0, splitedTitle[0].length()-1).split(",")));
			data.add(splitedTitle[1]);
			data.addAll(Arrays.asList(splitedTitle[2].substring(1, splitedTitle[2].length()).split(",")));
			this.movieData = data.toArray(new String[data.size()]);
		} else {
			this.movieData = movieString.split(",");
		}		
	}

	public String getMovieTitle() {
		return movieData[MOVIE_TITLE_POSITION];
	}


	public String getMovieCountry() {
		return movieData[MOVIE_COUNTRY_POSITION];
	}
	
	public boolean isValidMovie() {
		return movieData.length == MOVIE_DATA_LENGTH;
	}
	
	public int getNumberOfData() {
		return movieData.length;
	}
	
	public boolean isValidCountry() {
		return this.movieData[MOVIE_COUNTRY_POSITION] != null && !this.movieData[MOVIE_COUNTRY_POSITION].equals("");
	}
}
