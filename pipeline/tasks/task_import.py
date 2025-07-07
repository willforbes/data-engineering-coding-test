import os
import luigi
from pyspark.sql.functions import (
    monotonically_increasing_id,
    regexp_extract,
    col,
    to_date,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    LongType,
    TimestampType,
)
from utils.spark_utils import get_spark_session


class DataImportTask(luigi.Task):
    """
    Stage 1: Import CSV data, convert data types, save as parquet
    """

    # Luigi parameters
    csv_path = luigi.Parameter(default="resources/csv/allFilms.csv")
    schema_path = luigi.Parameter(default="resources/json/allFilesSchema.json")
    output_dir = luigi.Parameter(default="output")
    type_map = {
        "string": StringType(),
        "integer": IntegerType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "double": DoubleType(),
        "float": FloatType(),
        "long": LongType(),
        "timestamp": TimestampType(),
    }

    def output(self):
        """Define the output target"""
        return luigi.LocalTarget(os.path.join(self.output_dir, "films", "_SUCCESS"))

    def requires(self):
        """No dependencies for this task"""
        return []

    def run(self):
        """Execute the data import and conversion"""
        print("Starting Luigi DataImportTask")

        # Create Spark session
        spark = get_spark_session("LuigiFilmsDataImport")

        try:
            # Load schema
            schema = self._load_schema(self.schema_path)

            # Read CSV with header
            df_raw = spark.read.csv(self.csv_path, header=True, inferSchema=True)

            # Clean raw data to match schema
            df_cleaned = (
                df_raw.withColumn("id", monotonically_increasing_id())
                .withColumnRenamed("duration", "durationMins")
                .withColumn(
                    "durationMins",
                    regexp_extract(col("durationMins"), r"(\d+)", 1).cast("int"),
                )
                .withColumn("adult", col("adult").cast("boolean"))
                .withColumn("release_year", to_date(col("release_year"), "yyyy"))
                .select(
                    col("id"),
                    col("title"),
                    col("director"),
                    col("cast"),
                    col("country"),
                    col("release_year"),
                    col("adult"),
                    col("durationMins"),
                    col("genres"),
                    col("description"),
                )
            )

            df_final = spark.createDataFrame(df_cleaned.rdd, schema)

            print("CSV schema:")
            df_final.printSchema()

            print("CSV data:")
            df_final.show()

            # Create output directory
            os.makedirs(os.path.join(self.output_dir, "films"), exist_ok=True)

            # Save as parquet
            parquet_path = os.path.join(self.output_dir, "films")
            df_final.write.mode("overwrite").parquet(parquet_path)

            print(f"DataImportTask completed. Data saved to: {parquet_path}")
            print(f"Total records: {df_final.count()}")

        finally:
            spark.stop()

    def _load_schema(self, schema_path) -> StructType:
        """Load the schema from JSON file and convert to PySpark schema"""
        import json

        with open(schema_path, "r") as f:
            schema_json = json.load(f)

        # Convert JSON schema to PySpark schema
        fields = []
        for field in schema_json["fields"]:
            name = field["name"]
            data_type_str = field["type"].lower()
            nullable = field.get("nullable", True)
            data_type = self.type_map.get(data_type_str)
            if not data_type:
                raise ValueError(f"Unsupported type: {data_type_str} for field {name}")
            fields.append(StructField(name, data_type, nullable))
        return StructType(fields)
