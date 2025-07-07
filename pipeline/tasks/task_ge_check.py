import great_expectations as gx
import luigi
from pipeline.tasks.task_genre_split import GenreSplitCoordinatorTask
import os
import json


class DataCheckTask(luigi.Task):

    parquet_dir = luigi.Parameter(default="output")

    def output(self):
        """Output the validation results"""
        return luigi.LocalTarget(
            os.path.join(self.parquet_dir, "ge_validation_results.json")
        )

    def requires(self):
        """Requires the genre split coordinator task"""
        return GenreSplitCoordinatorTask(output_dir=self.parquet_dir)

    def run(self):
        context = gx.get_context(mode="ephemeral")
        datasource_name = "parquet"

        context.data_sources.add_spark_filesystem(
            name=datasource_name,
            base_directory=self.parquet_dir,
        )

        datasource = context.data_sources.get(datasource_name)
        films_asset = datasource.add_directory_parquet_asset(
            name="films",
            data_directory="films",
        )
        genres_asset = datasource.add_directory_parquet_asset(
            name="genres",
            data_directory="genres",
        )
        batch_defintion = films_asset.add_batch_definition_whole_directory(
            name="films_batch",
        )

        batch = batch_defintion.get_batch()
        print(batch.head())

        suite = gx.ExpectationSuite(name="films_suite")
        suite = context.suites.add(suite)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
        )
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))

        validation_definition = gx.ValidationDefinition(
            data=batch_defintion,
            suite=suite,
            name="films_validation",
        )
        validation_definition = context.validation_definitions.add(
            validation_definition
        )

        validation_result = validation_definition.run()
        print("Results:")
        print(validation_result)
        with self.output().open("w") as f:
            json.dump(validation_result.to_json_dict(), f, indent=2)
