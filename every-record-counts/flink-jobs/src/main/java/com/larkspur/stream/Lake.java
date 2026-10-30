package com.larkspur.stream;

import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

/** The two Iceberg tables the job writes, through Polaris as the job principal `flink` (Part 28's role). */
final class Lake {

    // Same catalog as Part 28's Flink SQL (flink/sql/part28/tables.sql), as a Java map.
    private static final Map<String, String> CATALOG = Map.of(
            "uri", "http://polaris:8181/api/catalog",
            "warehouse", "larkspur",
            "credential", "flink:flink-lab-secret",
            "scope", "PRINCIPAL_ROLE:ALL",
            "header.X-Iceberg-Access-Delegation", "vended-credentials",
            "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
            "s3.endpoint", "http://seaweedfs:8333",
            "s3.path-style-access", "true",
            "client.region", "us-east-1");

    private Lake() {}

    static TableLoader table(String name) {
        CatalogLoader catalog = CatalogLoader.rest("lake", new org.apache.hadoop.conf.Configuration(), CATALOG);
        return TableLoader.fromCatalog(catalog, TableIdentifier.parse(name));
    }

    /**
     * Append a stream to an Iceberg table with the default FlinkSink (not the experimental SinkV2 IcebergSink).
     * Writers flush data files at each checkpoint barrier; one committer commits them after the checkpoint completes.
     */
    static <T> DataStreamSink<Void> append(DataStream<T> stream, String name, MapFunction<T, RowData> toRow,
                                           String uidPrefix) {
        TableLoader loader = table(name);
        loader.open();
        Table t = loader.loadTable();
        FlinkSink.Builder b = FlinkSink
                .builderFor(stream, toRow, InternalTypeInfo.of(FlinkSchemaUtil.convert(t.schema())))
                .tableLoader(loader)
                // One writer, whatever the job's parallelism. The committer is pinned to parallelism 1 and max
                // parallelism 1; without this, rescaling moves the sink's last operator into the committer's task,
                // and a savepoint taken at another parallelism no longer restores (see the post's Troubleshooting).
                .writeParallelism(1);
        if (uidPrefix != null) {
            b.uidPrefix(uidPrefix);   // stable IDs for the writer and committer, whose state holds uncommitted files
        }
        return b.append();
    }

    static RowData alertRow(Alert a) {
        return GenericRowData.of(StringData.fromString(a.storeId), StringData.fromString(a.sku),
                StringData.fromString(a.kind), TimestampData.fromEpochMillis(a.eventTs),
                TimestampData.fromEpochMillis(a.detectedTs), a.level, a.opening, a.jobVersion);
    }

    static RowData lateRow(SaleLine l) {
        return GenericRowData.of(StringData.fromString(l.storeId), StringData.fromString(l.sku),
                StringData.fromString(l.basketId), l.qty, TimestampData.fromEpochMillis(l.eventTs),
                TimestampData.fromEpochMillis(l.uploadedAt), TimestampData.fromEpochMillis(l.watermark));
    }
}
