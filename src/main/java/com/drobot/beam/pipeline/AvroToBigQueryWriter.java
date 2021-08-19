package com.drobot.beam.pipeline;

import com.drobot.beam.schema.SchemaHolder;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

public class AvroToBigQueryWriter {

    private AvroToBigQueryWriter() {
    }

    @SuppressWarnings("unused")
    private interface Options extends PipelineOptions {
        @Description("Avro file template")
        @Default.String("gs://fdx-training-1/test-dataset.avro")
        ValueProvider<String> getFileTemplate();
        void setFileTemplate(ValueProvider<String> fileTemplate);

        @Description("BigQuery project ID")
        @Default.String("phase-one-322509")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> projectId);

        @Description("BigQuery dataset ID")
        @Default.String("phase_one")
        ValueProvider<String> getDatasetId();
        void setDatasetId(ValueProvider<String> datasetId);

        @Description("BigQuery table ID")
        @Default.String("first_table")
        ValueProvider<String> getTableId();
        void setTableId(ValueProvider<String> tableId);

        @Description("Location of GCS to store temporary files")
        @Default.String("gs://fdx-training-1/temp")
        ValueProvider<String> getGcsTempLocation();
        void setGcsTempLocation(ValueProvider<String> gcsTempLocation);
    }

    @SuppressWarnings("unused")
    private static class WriteToBigQueryDoFn extends DoFn<GenericRecord, BigQueryIO.Write<GenericRecord>> {

        private TableReference tableReference = null;
        private Options options = null;

        @StartBundle
        public void startBundle(StartBundleContext context, PipelineOptions options) {
            if (tableReference == null) {
                tableReference = createTableReference(context);
            }
            if (this.options == null) {
                this.options = options.as(Options.class);
            }
        }

        @ProcessElement
        public void processElement(@Element GenericRecord record,
                                   OutputReceiver<BigQueryIO.Write<GenericRecord>> output,
                                   ProcessContext context) {
            BigQueryIO.Write<GenericRecord> writer = write();
            output.output(writer);
        }

        private TableReference createTableReference(StartBundleContext context) {
            Options options = context.getPipelineOptions().as(Options.class);
            String projectId = options.getProjectId().get();
            String datasetId = options.getDatasetId().get();
            String tableId = options.getTableId().get();
            return new TableReference()
                    .setProjectId(projectId)
                    .setDatasetId(datasetId)
                    .setTableId(tableId);
        }

        private BigQueryIO.Write<GenericRecord> write() {
            SerializableFunction<GenericRecord, TableRow> function = BigQueryUtils.toTableRow(
                    AvroUtils.getGenericRecordToRowFunction(null)
            );
            return BigQueryIO.<GenericRecord>write()
                    .to(tableReference)
                    .withCustomGcsTempLocation(options.getGcsTempLocation())
                    .withSchema(SchemaHolder.getBqTableSchema())
                    .withFormatFunction(function)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE);
        }
    }

    public static Pipeline createPipeline(String... args) {
        Preconditions.checkNotNull(args, "Command line arguments are null");
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create()
                .as(Options.class);

        return Pipeline.create(options);
    }

    public static PipelineResult run(Pipeline pipeline) {
        Preconditions.checkNotNull(pipeline, "Pipeline reference is null");
        PCollection<GenericRecord> records = readRecords(pipeline);
        writeRecords(records);
        return pipeline.run();
    }

    private static PCollection<GenericRecord> readRecords(Pipeline pipeline) {
        AvroToBigQueryWriter.Options options = getOptions(pipeline);
        ValueProvider<String> gcsUrl = options.getFileTemplate();
        return pipeline.apply("Read avro files",
                AvroIO.readGenericRecords(SchemaHolder.getAvroRecordSchema()).from(gcsUrl)
        );
    }

    private static void writeRecords(PCollection<GenericRecord> records) {
        records.apply("Write records to BigQuery", ParDo.of(new WriteToBigQueryDoFn()));
    }

    private static Options getOptions(Pipeline pipeline) {
        return pipeline.getOptions().as(Options.class);
    }
}
