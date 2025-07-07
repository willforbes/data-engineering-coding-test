import os
import luigi
from pyspark.sql.functions import col, split, explode, trim
from pipeline.tasks.task_import import DataImportTask
from utils.spark_utils import get_spark_session
import re
import json


class GenreDiscoveryTask(luigi.Task):
    """
    Discovers all unique genres from the imported data
    """

    output_dir = luigi.Parameter(default="output")

    def output(self):
        """Output the list of discovered genres"""
        return luigi.LocalTarget(
            os.path.join(self.output_dir, "genres", "genre_list.json")
        )

    def requires(self):
        """Depends on DataImportTask completion"""
        return DataImportTask(output_dir=self.output_dir)

    def run(self):
        """Discover all unique genres"""
        print("Starting Luigi GenreDiscoveryTask")

        spark = get_spark_session("LuigiGenreDiscovery")

        try:
            # Read the data from Stage 1
            input_path = os.path.join(self.output_dir, "films")
            input_df = spark.read.parquet(input_path)

            # Create genres directory
            genres_dir = os.path.join(self.output_dir, "genres")
            os.makedirs(genres_dir, exist_ok=True)

            # Split genres and explode
            df_exploded = (
                input_df.withColumn("genre_list", split(trim(col("genres")), ","))
                .withColumn("genre", explode(col("genre_list")))
                .withColumn("genre", trim(col("genre")))
                .filter(col("genre").isNotNull() & (col("genre") != ""))
            )

            # Get unique genres
            unique_genres = df_exploded.select("genre").distinct().collect()
            genre_names = [row.genre for row in unique_genres]

            # Clean genre names
            clean_genres = []
            for genre in genre_names:
                clean_genre = self._clean_genre_name(genre)
                if clean_genre:
                    clean_genres.append({"original": genre, "clean": clean_genre})

            print(f"Found {len(clean_genres)} unique genres")

            # Save genre list to JSON
            with self.output().open("w") as f:
                json.dump(clean_genres, f, indent=2)

        finally:
            spark.stop()

    def _clean_genre_name(self, genre):
        """Clean and standardize genre names"""
        if not genre:
            return None

        # Remove common prefixes and clean up
        genre = genre.strip()
        genre = re.sub(r"^genre=", "", genre, flags=re.IGNORECASE)

        # Handle special characters and spaces
        genre = re.sub(r"[^\w\s]", "", genre)
        genre = re.sub(r"\s+", "_", genre.strip())

        return genre


class GenreSplitTask(luigi.Task):
    """
    Individual task to split data for a specific genre
    """

    output_dir = luigi.Parameter(default="output")
    genre_name = luigi.Parameter()
    original_genre = luigi.Parameter()

    def output(self):
        """Output for this specific genre"""
        return luigi.LocalTarget(
            os.path.join(
                self.output_dir, "genres", f"{self.genre_name}.parquet", "_SUCCESS"
            )
        )

    def requires(self):
        """Depends on DataImportTask completion"""
        return DataImportTask(output_dir=self.output_dir)

    def run(self):
        """Process data for this specific genre"""
        print(f"Starting Luigi GenreSplitTask for genre: {self.genre_name}")

        spark = get_spark_session(f"LuigiGenreSplit_{self.genre_name}")

        try:
            # Read the data from Stage 1
            input_path = os.path.join(self.output_dir, "films")
            input_df = spark.read.parquet(input_path)

            # Create genres directory
            genres_dir = os.path.join(self.output_dir, "genres")
            os.makedirs(genres_dir, exist_ok=True)

            # Split genres and explode
            df_exploded = (
                input_df.withColumn("genre_list", split(trim(col("genres")), ","))
                .withColumn("genre", explode(col("genre_list")))
                .withColumn("genre", trim(col("genre")))
                .filter(col("genre") == self.original_genre)
                .drop("genre", "genre_list")
            )

            # Save to parquet
            genre_file_path = os.path.join(genres_dir, f"{self.genre_name}.parquet")
            df_exploded.write.mode("overwrite").parquet(genre_file_path)

            count = df_exploded.count()
            print(
                f"Saved {count} records for genre '{self.original_genre}' to {genre_file_path}"
            )

        finally:
            spark.stop()


class GenreSplitCoordinatorTask(luigi.Task):
    """
    Coordinates the creation of individual genre split tasks
    """

    output_dir = luigi.Parameter(default="output")

    def output(self):
        """Output a success file indicating all genres have been processed"""
        return luigi.LocalTarget(os.path.join(self.output_dir, "genres", "_SUCCESS"))

    def requires(self):
        """Requires the genre discovery task"""
        return GenreDiscoveryTask(output_dir=self.output_dir)

    def run(self):
        """This task coordinates by reading the genre list and creating individual tasks"""
        print("GenreSplitCoordinatorTask starting - reading discovered genres")

        # Read the discovered genres from the JSON file
        with self.input().open("r") as f:
            genres_data = json.load(f)

        print(f"Found {len(genres_data)} genres to process")

        # Create individual tasks for each genre and run them
        genre_tasks = []
        for genre_info in genres_data:
            task = GenreSplitTask(
                output_dir=self.output_dir,
                genre_name=genre_info["clean"],
                original_genre=genre_info["original"],
            )
            genre_tasks.append(task)

        # Run all genre tasks
        luigi.build(genre_tasks, local_scheduler=True)

        print("GenreSplitCoordinatorTask completed - all genres processed")
        # Create the success file
        with self.output().open("w") as f:
            f.write("All genres processed successfully\n")
