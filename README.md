# Filter Movies with Ratings Above Threshold

This module filters out movies whose **average rating** is below a specified threshold (e.g., 4.0) and retains only those with an **average rating >= threshold**.

---

## Objective

- **Goal:** Identify top-performing movies based on audience feedback by filtering out movies that have an average rating below the threshold.
- **Threshold Example:** 4.0

---

## Use Cases

1. **Movie Recommendation Systems**  
   - Recommend only highly-rated movies to users.

2. **Analytics**  
   - Identify top-performing movies based on audience feedback.

3. **Data Cleaning**  
   - Remove movies with consistently poor ratings.

---

## MapReduce Workflow

### Mapper

- **Input:** `ratings.csv` (or similar dataset)
- **Key:** `movieId`
- **Value:** `rating`

The mapper emits key-value pairs of `(movieId, rating)`. During the shuffle and sort phase, all ratings for the same `movieId` are grouped together for the reducer.

### Reducer

- **Input:** `(movieId, [list_of_ratings])`
- **Process:** Calculate the **average rating** for each movie.
- **Decision:** Compare the average rating to the threshold (e.g., 4.0).
  - If `averageRating >= threshold`, **emit** `(movieId, averageRating)`.

The reducer’s final output contains only the movies whose average ratings meet or exceed the threshold.

---

## Running the Job

1. **Build the Jar**  
   If you have already built the project, the **uber-jar** should be located at:
   ```bash
   C:\Users\tarun\Documents\GitHub\INFO_7250_Big_data\final_project\FilterMoviesDriver\target\FilterMoviesDriver-1.0-SNAPSHOT.jar
   scp /mnt/c/Users/tarun/Documents/GitHub/INFO_7250_Big_data/final_project/FilterMoviesDriver/target/FilterMoviesDriver-1.0-SNAPSHOT.jar hdoop@tarunsankhla:~/FilterMoviesDriver-1.0-SNAPSHOT.jar
   hadoop jar ~/FilterMoviesDriver-1.0-SNAPSHOT.jar edu.neu.csye6220.filtermoviesdriver.FilterMoviesDriver /final_project/ratings.csv /user/hdoop/filter_movies_threshold
   hdfs dfs -cat /user/hdoop/filter_movies_threshold/part-r-00000 | head
   hdfs dfs -ls /user/hdoop/filter_movies_threshold
   ```


![image](https://github.com/user-attachments/assets/0fc76a4e-c255-47a0-a5cd-2dff29f4d0d9)
