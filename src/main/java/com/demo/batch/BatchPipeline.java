package com.demo.batch;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.demo.batch.config.BatchPipelineOptions;
import com.demo.batch.dofn.ParseToTableRow;
import com.demo.batch.dofn.ValidateStreetcarBusData;
import com.demo.batch.dofn.ValidateSubwayData;
import com.demo.batch.model.TransitDelay;
import com.demo.batch.model.TransitDelay.TransitType;
import com.google.api.services.bigquery.model.TableRow;

/**
 * <h1>Batch Pipeline Construction</h1>
 * 
 * Construction of Batch Pipeline using a Dataflow template (via Value Provider).  Pipeline reads
 * 3 different delay data sources [Subway; Street Car; Bus], parsed into the common class TransitDelay
 * and written into BigQuery 
 *  
 * <h2>Side Input</h2>
 * 
 * <p>A side input is constructed for the Subway Delay data as 'Reason For Delay' is coded.</p>
 * 
 * <p>The below separates the two column CSV into a map element and creates a PCollectView(Map)
 * from the result.  This PCollectionView is used as a side input downstream</p>
 * 
 * <p><b>Note:</b> DoFn depending on side inputs will only run after the PCollectionView is complete</p>
 *
 * <h2>Reshuffle</h2>
 * 
 * </p> See <a href="https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/transforms/Reshuffle.html">
 * Reshuffle JavaDoc</a></p>
 * 
 * <p>Reshuffle, similar to GroupByKey and Combine, implements a 'fusion break'. In short, the parallelization of
 * Dataflow assumes number of outputs from DoFn are small and optimizes the runner for these cases.  When the
 * DoFn produces a large number of output, the parallelization is limited (work is not spread out as much and
 * affects the autoscaling). Thus, using a reshuffle after this or even before a time-consuming DoFn (to ensure
 * all threads are working in parallel) improves the performance of the pipeline. For more details, refer to:
 * <a href="https://beam.apache.org/contribute/ptransform-style-guide/#performance">Beam Performance</a></p>
 * 
 * <p>In this example, each output from TextIO can lead to thousands of outputs.  Using a 'fusion break' will
 * ensure the workload is further parallelized.</p>
 * 
 * @author vax.thurai
 * @version 1.0
 */

public class BatchPipeline {
	public static Pipeline build(Pipeline p) {
		
		/*
		 * The options consists of the DataflowOptions and additional User-defined
		 * options specified in BatchPipelineOptions.  This option will feed the processes
		 * at RunTime using ValueProviders.
		 */
		BatchPipelineOptions pipelineOptions = (BatchPipelineOptions) p.getOptions();
		

		PCollectionView<Map<String, String>> subwayCodeMapping = p
			.apply("Retrieve Codes", TextIO.read()
					.from(pipelineOptions.getSubwayLogCodes()))
			.apply("Parse into Map", MapElements
					.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
					.via(input -> KV.of(input.split(",")[0], input.split(",")[1])))
			.apply("Create Side Input", View.asMap());
				
		@SuppressWarnings("deprecation")
		PCollection<TableRow> subwayData = p
			.apply("Read from TTC Subway Delay Folder", TextIO.read()
					.from(pipelineOptions.getSubwayDelayFilePrefix()))
			.apply(Reshuffle.viaRandomKey())
			.apply("Validate Subway Data", ParDo.of(new ValidateSubwayData(subwayCodeMapping))
					.withSideInputs(subwayCodeMapping))
					.setCoder(AvroCoder.of(TransitDelay.class))
			.apply("Parse To Table Row", ParDo.of(new ParseToTableRow()));

		/*
		 * Reads street car CSV data, validates the data, and parses into BigQuery TableRow object. 
		 */
		@SuppressWarnings("deprecation")
		PCollection<TableRow> streetcarData = p
				.apply("Read from TTC Streetcar Delay Folder", TextIO.read()
						.from(pipelineOptions.getStreetcarDelayFilePrefix()))
				.apply(Reshuffle.viaRandomKey())
				.apply("Validate Streetcar Data", ParDo.of(new ValidateStreetcarBusData(TransitType.STREETCAR)))
						.setCoder(AvroCoder.of(TransitDelay.class))
				.apply("Parse To Table Row", ParDo.of(new ParseToTableRow()));

		/*
		 * Reads bus CSV data, validates the data, and parses into BigQuery TableRow object. 
		 */
		@SuppressWarnings("deprecation")
		PCollection<TableRow> busData = p
				.apply("Read from TTC Bus Delay Folder", TextIO.read()
						.from(pipelineOptions.getBusDelayFilePrefix()))
				.apply(Reshuffle.viaRandomKey())
				.apply("Validate Bus Data", ParDo.of(new ValidateStreetcarBusData(TransitType.BUS)))
						.setCoder(AvroCoder.of(TransitDelay.class))
				.apply("Parse To Table Row", ParDo.of(new ParseToTableRow()));

				
		PCollectionList.<TableRow>of(subwayData)
				.and(streetcarData)
				.and(busData)
				.apply(Flatten.pCollections())
				.apply(BigQueryIO.writeTableRows()
						.to(pipelineOptions.getTableSpec())
						.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
						.withCustomGcsTempLocation(pipelineOptions.getCustomGcsTempLocation())
						.withSchema(TransitDelay.getTransitDelaySchema()));

		return p;
	}
}
