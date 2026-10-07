from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType
)

# =========================================================
# 1. Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("Kafka_To_Iceberg_Nessie_CDC")

    # =====================================================
    # Iceberg Spark Extensions
    # =====================================================
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # =====================================================
    # Nessie Catalog
    # =====================================================
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    .config(
        "spark.sql.catalog.lakehouse.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog"
    )

    .config(
        "spark.sql.catalog.lakehouse.uri",
        "http://nessie:19120/api/v2"
    )

    .config(
        "spark.sql.catalog.lakehouse.ref",
        "main"
    )

    .config(
        "spark.sql.catalog.lakehouse.authentication.type",
        "NONE"
    )

    .config(
        "spark.sql.catalog.lakehouse.io-impl",
        "org.apache.iceberg.hadoop.HadoopFileIO"
    )

    # =====================================================
    # SeaweedFS / S3 Warehouse
    # =====================================================
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3a://warehouse/iceberg/"
    )

    # =====================================================
    # S3A Configuration
    # =====================================================
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://seaweedfs:8333"
    )

    .config(
        "spark.hadoop.fs.s3a.access.key",
        "admin"
    )

    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "secret123"
    )

    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )

    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 2. Create Iceberg Namespace
# =========================================================

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.ecommerce
""")


# =========================================================
# 3. Create Iceberg Customers Table
# =========================================================

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.ecommerce.customers (
        customer_id INT,
        name STRING,
        email STRING,
        city STRING,
        updated_at BIGINT
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '2'
    )
""")


# =========================================================
# 4. Read CDC Events From Kafka
# =========================================================

kafka_df = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    )

    .option(
        "subscribe",
        "ecommerce.public.customers"
    )

    .option(
        "startingOffsets",
        "earliest"
    )

    .load()
)


# =========================================================
# 5. Debezium Customer Schema
# =========================================================

customer_schema = StructType([
    StructField(
        "customer_id",
        IntegerType()
    ),

    StructField(
        "name",
        StringType()
    ),

    StructField(
        "email",
        StringType()
    ),

    StructField(
        "city",
        StringType()
    ),

    StructField(
        "updated_at",
        LongType()
    )
])


# =========================================================
# 6. Debezium Payload Schema
# =========================================================

debezium_payload_schema = StructType([

    StructField(
        "before",
        customer_schema
    ),

    StructField(
        "after",
        customer_schema
    ),

    StructField(
        "op",
        StringType()
    ),

    StructField(
        "ts_ms",
        LongType()
    ),

    StructField(
        "ts_us",
        LongType()
    ),

    StructField(
        "ts_ns",
        LongType()
    )
])


# =========================================================
# 7. Parse Kafka JSON Payload
# =========================================================

parsed_df = (
    kafka_df

    .selectExpr(
        "CAST(value AS STRING) AS json_value"
    )

    .select(
        from_json(
            col("json_value"),

            StructType([
                StructField(
                    "payload",
                    debezium_payload_schema
                )
            ])

        ).alias("data")
    )

    .select(
        "data.payload.*"
    )
)


# =========================================================
# 8. CDC -> Iceberg MERGE
# =========================================================

def merge_cdc_to_iceberg(batch_df, batch_id):

    # -----------------------------------------------------
    # Ignore empty micro-batches
    # -----------------------------------------------------

    if batch_df.isEmpty():
        return

    print(
        f"========== Processing Batch {batch_id} =========="
    )

    # -----------------------------------------------------
    # IMPORTANT:
    # Use the SparkSession associated with this batch.
    # -----------------------------------------------------

    session = batch_df.sparkSession

    # -----------------------------------------------------
    # Register current micro-batch as temporary view
    # -----------------------------------------------------

    batch_df.createOrReplaceTempView(
        "raw_batch"
    )

    # -----------------------------------------------------
    # Deduplicate CDC events
    #
    # If multiple events for the same customer exist
    # inside the same micro-batch, keep the latest one.
    # -----------------------------------------------------

    session.sql("""
        CREATE OR REPLACE TEMP VIEW batch_updates AS

        SELECT *
        FROM (

            SELECT
                *,

                ROW_NUMBER() OVER (
                    PARTITION BY
                        COALESCE(
                            after.customer_id,
                            before.customer_id
                        )

                    ORDER BY
                        ts_ms DESC
                ) AS rn

            FROM raw_batch

        )

        WHERE rn = 1
    """)

    # -----------------------------------------------------
    # MERGE CDC Events Into Iceberg
    # -----------------------------------------------------

    session.sql("""
        MERGE INTO
            lakehouse.ecommerce.customers AS t

        USING (

            SELECT

                op,

                COALESCE(
                    after.customer_id,
                    before.customer_id
                ) AS customer_id,

                after.name AS name,

                after.email AS email,

                after.city AS city,

                after.updated_at AS updated_at

            FROM batch_updates

        ) AS s

        ON t.customer_id = s.customer_id


        -- DELETE
        WHEN MATCHED
             AND s.op = 'd'

        THEN DELETE


        -- UPDATE
        WHEN MATCHED
             AND s.op IN ('u', 'c')

        THEN UPDATE SET

            t.name = s.name,

            t.email = s.email,

            t.city = s.city,

            t.updated_at = s.updated_at


        -- INSERT
        WHEN NOT MATCHED
             AND s.op IN ('c', 'u', 'r')

        THEN INSERT (
            customer_id,
            name,
            email,
            city,
            updated_at
        )

        VALUES (
            s.customer_id,
            s.name,
            s.email,
            s.city,
            s.updated_at
        )
    """)

    print(
        f"========== Batch {batch_id} Completed =========="
    )


# =========================================================
# 9. Start Streaming Query
# =========================================================

query = (
    parsed_df

    .writeStream

    .foreachBatch(
        merge_cdc_to_iceberg
    )

    .option(
        "checkpointLocation",
        "s3a://warehouse/checkpoints/iceberg_customers_cdc"
    )

    .start()
)


# =========================================================
# 10. Keep Streaming Query Alive
# =========================================================

query.awaitTermination()