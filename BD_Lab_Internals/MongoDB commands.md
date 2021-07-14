# Question 3

## 1) Demonstrate the usage of $match, $group, aggregate pipelines.

```{mongo}
db.movie.aggregate({$match:{referable:false}})
```

```{mongo}
db.movie.aggregate({$group:{_id:"$genre",number_of_movies:{$sum:1}}})
```

```{mongo}
db.movie.aggregate({$match:{referable:false}},{$group:{_id:"$genre",total_positive_fed:{$sum:1}}})
```


## 2) Demonstrate the Map-Reduce aggregate function on thi dataset.
```{mongo}
var mapper=function(){emit(this.movie_name,this.IMDB)};
```
```{mongo}
var reducer=function(movie_name,IMDB){return Array.sum(IMDB)};
```
```{mongo}
db.movie.mapReduce(mapper,reducer,{out:myoutput})
```

## 3) Count the number of Movies which belongs to the Thriller category and find out the total number of positive reviews in that category
```{mongo}
db.movie.aggregate({$match:{genre:"Thriller"}},{$group:{_id:"$genre",number_of_movies:{$sum:1},total_positive_feedback:{$sum:$"positive_fed}}})
```
## 4) Group all the records by genre and find out total number of positive feedbacks by genre.
```{mongo}
db.movie.aggregate({$group:{_id:"$genre",total_positive_feedback:{$sum:"$positive_fed"}}})
