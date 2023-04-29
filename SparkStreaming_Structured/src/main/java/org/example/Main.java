package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder()
                .appName("Hospital Incident Streaming App")
                .master("local[*]")
                .getOrCreate();
        StructType serviceSchema = new StructType(new StructField[]{
                new StructField("Id", DataTypes.StringType,true, Metadata.empty()),
                new StructField("Titre", DataTypes.StringType,true, Metadata.empty()),
                new StructField("Description", DataTypes.StringType,true, Metadata.empty()),
                new StructField("Service", DataTypes.StringType,true, Metadata.empty()),
                new StructField("Date", DataTypes.StringType,true, Metadata.empty())
        });
        Dataset<Row> DfServiceHopital = spark.readStream()
                .option("header",true)
                .schema(serviceSchema)
                .csv("files");

        // Tâche 1: Afficher le nombre d'incidents par service
        Dataset<Row> countByService = DfServiceHopital.groupBy("Service")
                .agg(functions.count("*").as("total"))
                .orderBy(functions.desc("total"));

        countByService.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        // Tâche 2: Afficher les deux années avec le plus grand nombre d'incidents
        Dataset<Row> incidentsByYear = DfServiceHopital.withColumn("Date", functions.split(DfServiceHopital.col("Date"),"-",0))
                .groupBy("Date")
                .agg(functions.count("*").as("total"))
                .orderBy(functions.desc("total"));

        incidentsByYear.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime(3000))
                .start();
        spark.streams().awaitAnyTermination();
    }
}