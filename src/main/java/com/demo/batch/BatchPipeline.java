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

public class BatchPipeline {
	public static Pipeline build(Pipeline p) {
		
		/*
		 * The pipeline options are constructed into the Dataflow Template alongside the structure
		 * of the pipeline.  The options consists of the DataflowOptions and additional User-defined
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
			
		@SuppressWarnings("deprecation")
		PCollection<TableRow> streetcarData = p
				.apply("Read from TTC Streetcar Delay Folder", TextIO.read()
						.from(pipelineOptions.getStreetcarDelayFilePrefix()))
				.apply(Reshuffle.viaRandomKey())
				.apply("Validate Streetcar Data", ParDo.of(new ValidateStreetcarBusData(TransitType.STREETCAR)))
						.setCoder(AvroCoder.of(TransitDelay.class))
				.apply("Parse To Table Row", ParDo.of(new ParseToTableRow()));

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
