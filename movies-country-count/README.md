# Peliculas
En este proyecto se encontran ejemplo en los que se ha trabajado con un dataset de peliculas.

## Dataset
El dataset se puede descargar de: (movies dataset) [https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset]

## Compilacion del proyecto
Para compilar el proyecto
```
mvn install
```

## Ejecución del proyecto
Para probar el proyecto hay que diferenciar entre los diferentes ejecicios

### Contador peliculas por pais
Ejecucion:
```
hadoop jar hadoop-movies-examples.jar es.own3dh2so4.hadoop.movie.MovieCountryCountDriver inputFolder outputFolder
```