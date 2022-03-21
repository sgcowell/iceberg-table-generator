import static org.apache.iceberg.Files.localInput;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class IcebergTableGenerator {

  private final HadoopCatalog catalog;
  private final Path warehousePath;
  private final ValueGenerator generator;
  private final TableIdentifier id;
  private Table table;
  private Transaction transaction;

  public IcebergTableGenerator(String warehousePath, TableIdentifier id) {
    Configuration conf = new Configuration();
    this.catalog = new HadoopCatalog();
    this.catalog.setConf(conf);
    this.catalog.initialize(
        "hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehousePath));
    this.warehousePath = Paths.get(warehousePath);
    this.generator = new ValueGenerator(42);
    this.id = id;
  }

  public IcebergTableGenerator create(Schema schema, PartitionSpec partitionSpec) {
    return create(schema, partitionSpec, ImmutableMap.of());
  }

  public IcebergTableGenerator create(
      Schema schema, PartitionSpec partitionSpec, Map<String, String> tableProperties) {
    System.out.format("Creating '%s'...\n", id.toString());
    if (catalog.tableExists(id)) {
      catalog.dropTable(id, true);
    }

    Map<String, String> updatedProperties = new HashMap<>(tableProperties);
    updatedProperties.computeIfAbsent(TableProperties.FORMAT_VERSION, k -> "2");

    table = catalog.createTable(id, schema, partitionSpec, updatedProperties);

    return this;
  }

  public <T> IcebergTableGenerator append(
      List<T> partitionValues,
      RecordGenerator<T> recordGenerator,
      int dataFilesPerPartition,
      int rowsPerDataFile)
      throws IOException {
    Preconditions.checkState(table != null, "create must be called first");
    Path dataDir = getDataDirectory(id);
    AppendFiles appendFiles = getTransaction().newAppend();

    for (T value : partitionValues) {
      Path partitionDir = dataDir.resolve(value.toString());
      for (int fileNum = 0; fileNum < dataFilesPerPartition; fileNum++) {
        String fileName = value + "-" + String.format("%02d", fileNum) + ".parquet";
        File parquetFile = partitionDir.resolve(fileName).toFile();
        appendFiles.appendFile(writeDataFile(parquetFile, value, recordGenerator, rowsPerDataFile));
      }
    }

    appendFiles.commit();

    return this;
  }

  public <T> IcebergTableGenerator mergeOnReadDelete(
      List<T> partitionValues, Predicate<Record> deletePredicate) throws IOException {
    Path dataDir = getDataDirectory(id);
    PartitionField field = table.spec().fields().get(0);
    Expression expr = Expressions.in(field.name(), partitionValues.toArray());
    CloseableIterable<FileScanTask> scanTasks = table.newScan().filter(expr).planFiles();

    RowDelta rowDelta = getTransaction().newRowDelta();

    Map<PartitionKey, List<FileScanTask>> orderedTasks =
        orderFileScanTasksByPartitionAndPath(scanTasks);
    for (PartitionKey key : orderedTasks.keySet()) {
      String partitionString = partitionKeyDirectoryName(key);
      Path partitionDir = dataDir.resolve(partitionString);
      File deleteFile =
          getUniqueNumberedFilename(partitionDir + "/delete-" + partitionString + "-%02d.parquet");

      OutputFile out = Files.localOutput(deleteFile);
      PositionDeleteWriter<Record> deleteWriter =
          Parquet.writeDeletes(out)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .overwrite()
              .rowSchema(table.schema())
              .withSpec(table.spec())
              .withPartition(key)
              .setAll(table.properties())
              .buildPositionWriter();
      try (PositionDeleteWriter<Record> writer = deleteWriter) {
        for (FileScanTask task : orderedTasks.get(key)) {
          String filePath = task.file().path().toString();
          try (CloseableIterable<Record> reader =
              Parquet.read(Files.localInput(filePath))
                  .project(table.schema())
                  .reuseContainers()
                  .createReaderFunc(
                      fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                  .build()) {

            int pos = 0;
            for (Record record : reader) {
              if (deletePredicate.test(record)) {
                writer.delete(filePath, pos, record);
              }
              pos++;
            }
          }
        }
      }

      rowDelta.addDeletes(deleteWriter.toDeleteFile());
    }

    rowDelta.commit();
    return this;
  }

  public IcebergTableGenerator commit() {
    transaction.commitTransaction();
    transaction = null;
    return this;
  }

  private Path getDataDirectory(TableIdentifier id) {
    return warehousePath.resolve(Paths.get(id.toString().replace(".", "/"), "data"));
  }

  private File getUniqueNumberedFilename(String template) {
    int fileNum = 0;
    File name;
    do {
      name = new File(String.format(template, fileNum));
      fileNum++;
    } while (name.exists());

    return name;
  }

  private Transaction getTransaction() {
    if (transaction == null) {
      transaction = table.newTransaction();
    }

    return transaction;
  }

  private <T> DataFile writeDataFile(
      File parquetFile, T partitionValue, RecordGenerator<T> recordGenerator, int nrows)
      throws IOException {
    try (FileAppender<GenericRecord> appender =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .setAll(table.properties())
            .build()) {
      Stream<GenericRecord> stream =
          Stream.iterate(0, i -> i + 1)
              .limit(nrows)
              .map(i -> recordGenerator.next(generator, partitionValue));
      appender.addAll(stream.iterator());
      appender.close();

      PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
      partitionKey.set(0, partitionValue);

      return DataFiles.builder(table.spec())
          .withPartition(partitionKey)
          .withInputFile(localInput(parquetFile))
          .withMetrics(appender.metrics())
          .withFormat(FileFormat.PARQUET)
          .build();
    }
  }

  private Map<PartitionKey, List<FileScanTask>> orderFileScanTasksByPartitionAndPath(
      CloseableIterable<FileScanTask> scanTasks) {
    Map<PartitionKey, List<FileScanTask>> map = new HashMap<>();
    for (FileScanTask task : scanTasks) {
      PartitionKey key = getPartitionKey(task);
      map.computeIfAbsent(key, k -> new ArrayList<>()).add(task);
    }

    for (PartitionKey key : map.keySet()) {
      map.get(key).sort(Comparator.comparing(t -> t.file().path(), CharSequence::compare));
    }

    return map;
  }

  private PartitionKey getPartitionKey(FileScanTask scanTask) {
    PartitionKey partitionKey = new PartitionKey(scanTask.spec(), table.schema());
    for (int i = 0; i < scanTask.file().partition().size(); i++) {
      partitionKey.set(i, scanTask.file().partition().get(i, Object.class));
    }

    return partitionKey;
  }

  private String partitionKeyDirectoryName(PartitionKey partitionKey) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < partitionKey.size(); i++) {
      if (i > 0) {
        builder.append("-");
      }
      builder.append(partitionKey.get(i, Object.class));
    }

    return builder.toString();
  }
}
