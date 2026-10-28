package com.larkspur.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

/**
 * Part 7: what an Iceberg commit is, one API call at a time.
 *
 * <p>On a small store-closure table: A writes a file and commits it; two writers then append at the same moment
 * (the catalog refuses some pointer swaps and Iceberg retries them); C, which read the table before those appends,
 * tries to replace October's rows and fails validation. Needs `make up-core`.
 */
public final class CommitAnatomy {

    private static final TableIdentifier ID = TableIdentifier.of("part07", "store_closure");
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "store_id", Types.StringType.get()),
            Types.NestedField.required(2, "business_date", Types.DateType.get()),
            Types.NestedField.required(3, "reason", Types.StringType.get()));
    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).month("business_date").build();

    public static void main(String[] args) throws IOException {
        try (RESTCatalog catalog = new RESTCatalog()) {
            catalog.initialize("lab", Map.ofEntries(
                    Map.entry(CatalogProperties.URI, "http://localhost:8181/api/catalog"),
                    Map.entry(CatalogProperties.WAREHOUSE_LOCATION, "larkspur"),
                    Map.entry("credential", "root:s3cr3t"),
                    Map.entry("scope", "PRINCIPAL_ROLE:ALL"),
                    Map.entry(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO"),
                    Map.entry("s3.endpoint", "http://localhost:8333"),
                    Map.entry("s3.path-style-access", "true"),
                    Map.entry("s3.access-key-id", "larkspur"),
                    Map.entry("s3.secret-access-key", "larkspur-lab-secret"),
                    Map.entry("client.region", "us-east-1"),
                    Map.entry("rest.auth.type", "oauth2"),
                    Map.entry("oauth2-server-uri", "http://localhost:8181/api/catalog/v1/oauth/tokens"),
                    Map.entry(CatalogProperties.METRICS_REPORTER_IMPL, AttemptCounter.class.getName())));
            if (!catalog.namespaceExists(Namespace.of("part07"))) {
                catalog.createNamespace(Namespace.of("part07"));
            }
            if (catalog.tableExists(ID)) {  // start clean. Polaris refuses purge by default, so delete the files here
                Table old = catalog.loadTable(ID);
                catalog.dropTable(ID, false);
                ((SupportsPrefixOperations) old.io()).deletePrefix(old.location());
            }
            catalog.createTable(ID, SCHEMA, SPEC);

            // 1. Write a data file, then commit it. Until commit() swaps the catalog's pointer, no reader sees it.
            Table a = catalog.loadTable(ID);
            DataFile refit = write(a, "GB-0042", LocalDate.of(2026, 9, 30), "refit");   // a Parquet file nobody can see yet
            System.out.printf("A wrote %s (%d bytes, %d row)%n",
                    tail(refit.location()), refit.fileSizeInBytes(), refit.recordCount());
            String before = metadataLocation(a);
            a.newAppend().appendFile(refit).commit();                                   // the pointer swap
            System.out.printf("A committed snapshot %d: %s -> %s%n",
                    a.currentSnapshot().snapshotId(), tail(before), tail(metadataLocation(a)));

            // 2. C reads the table now and plans to replace October's closures later.
            Table c = catalog.loadTable(ID);
            long base = c.currentSnapshot().snapshotId();
            System.out.printf("C read snapshot %d%n", base);

            // 3. Two writers append at the same moment, ten commits each. When the catalog refuses a pointer swap,
            //    Iceberg re-reads the table and re-applies the append on top: appends never conflict.
            AttemptCounter.reset();
            try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
                for (String country : List.of("GB", "IN")) {
                    pool.submit(() -> {
                        Table t = catalog.loadTable(ID);
                        for (int i = 1; i <= 10; i++) {
                            t.newAppend().appendFile(write(t, "%s-%04d".formatted(country, i),
                                    LocalDate.of(2026, 10, i), "inspection")).commit();
                        }
                        return null;
                    });
                }
            }
            System.out.printf("2 writers x 10 appends at once: %d commits in %d attempts%n",
                    AttemptCounter.COMMITS.get(), AttemptCounter.ATTEMPTS.get());

            // 4. C replaces October, but only if nobody added October rows after the snapshot it read.
            Expression october = Expressions.and(
                    Expressions.greaterThanOrEqual("business_date", "2026-10-01"),
                    Expressions.lessThan("business_date", "2026-11-01"));
            try {
                c.newOverwrite()
                        .overwriteByRowFilter(october)
                        .addFile(write(c, "SG-0007", LocalDate.of(2026, 10, 1), "power cut"))
                        .validateFromSnapshot(base)          // what C read before deciding
                        .conflictDetectionFilter(october)    // the rows C's decision depends on
                        .validateNoConflictingData()         // fail if anyone added such rows since
                        .commit();
                System.out.println("C committed");
            } catch (ValidationException e) {
                String message = e.getMessage();
                System.out.printf("C failed: ValidationException: %s ...%n", message.substring(0, message.indexOf(": [")));
            }
            System.out.printf("history: %d snapshots%n", catalog.loadTable(ID).history().size());
        }
    }

    /** Counts commits and commit attempts from Iceberg's own commit reports. */
    public static final class AttemptCounter implements MetricsReporter {
        static final AtomicLong COMMITS = new AtomicLong();
        static final AtomicLong ATTEMPTS = new AtomicLong();

        static void reset() {
            COMMITS.set(0);
            ATTEMPTS.set(0);
        }

        @Override
        public void report(MetricsReport report) {
            if (report instanceof CommitReport commit) {
                COMMITS.incrementAndGet();
                ATTEMPTS.addAndGet(commit.commitMetrics().attempts().value());
            }
        }
    }

    /** Write one row as a Parquet data file in the table's location and describe it as a DataFile. */
    private static DataFile write(Table table, String store, LocalDate day, String reason) {
        Record row = GenericRecord.create(SCHEMA);
        row.setField("store_id", store);
        row.setField("business_date", day);
        row.setField("reason", reason);
        PartitionKey partition = new PartitionKey(table.spec(), table.schema());
        partition.partition(new InternalRecordWrapper(table.schema().asStruct()).wrap(row));
        // The table decides where the file goes and how it's written (Parquet, its compression, its column IDs).
        EncryptedOutputFile out = OutputFileFactory.builderFor(table, 1, System.nanoTime())
                .format(FileFormat.PARQUET).build()
                .newOutputFile(table.spec(), partition);
        DataWriter<Record> writer = new GenericFileWriterFactory.Builder(table)
                .dataFileFormat(FileFormat.PARQUET).build()
                .newDataWriter(out, table.spec(), partition);
        try (writer) {
            writer.write(row);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return writer.toDataFile();
    }

    private static String metadataLocation(Table table) {
        return ((org.apache.iceberg.HasTableOperations) table).operations().current().metadataFileLocation();
    }

    private static String tail(String path) {
        return path.substring(path.lastIndexOf('/', path.lastIndexOf('/') - 1) + 1);
    }

    private CommitAnatomy() {
    }
}
