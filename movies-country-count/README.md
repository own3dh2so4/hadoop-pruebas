# Peliculas
En este proyecto se encontran ejemplo en los que se ha trabajado con un dataset de peliculas.

## Dataset
El dataset se puede descargar de: [movies dataset](https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset)

## Compilacion del proyecto
Para compilar el proyecto
```
mvn install
```

## Ejecución del proyecto
Para probar el proyecto hay que diferenciar entre los diferentes ejecicios

### Contador total de peliculas
Ejecucion:
```
hadoop jar hadoop-movies-examples.jar es.own3dh2so4.hadoop.movie.totalcount.MovieTotalCountDriver <inputFolder> <outputFolder>
```
Ejemplo:
```
hadoop jar hadoop-movies-examples.jar es.own3dh2so4.hadoop.movie.totalcount.MovieTotalCountDriver in/movies out/movies/count
```

### Contador peliculas por pais
Ejecucion:
```
hadoop jar hadoop-movies-examples.jar es.own3dh2so4.hadoop.movie.countrycount.MovieCountryCountDriver <inputFolder> <outputFolder>
```
Ejemplo:
```
hadoop jar hadoop-movies-examples.jar es.own3dh2so4.hadoop.movie.countrycount.MovieCountryCountDriver in/movies out/movies/count
```