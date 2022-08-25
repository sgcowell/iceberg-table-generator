import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.parquet.column.ParquetProperties;

public class IcebergTableGenerator {

  private final HadoopCatalog catalog;
  private final String warehousePath;
  private final ValueGenerator generator;
  private final TableIdentifier id;
  private Table table;
  private Transaction transaction;

  public IcebergTableGenerator(String warehousePath, Configuration conf, TableIdentifier id) {
    this.catalog = new HadoopCatalog();
    this.catalog.setConf(conf);
    this.catalog.initialize(
        "hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehousePath));
    this.warehousePath = warehousePath;
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

  public Table getTable() {
    return table;
  }

  public IcebergTableGenerator updateSpec(List<Term> additions, List<Term> removals) {
    UpdatePartitionSpec update = getTransaction().updateSpec();
    additions.forEach(update::addField);
    removals.forEach(update::removeField);
    update.commit();

    return this;
  }

  public <T> IcebergTableGenerator append(
      List<T> partitionValues,
      RecordGenerator<T> recordGenerator,
      int dataFilesPerPartition,
      int rowsPerDataFile)
      throws IOException {
    Preconditions.checkState(table != null, "create must be called first");
    URI dataDir = getDataDirectory(id);
    AppendFiles appendFiles = getTransaction().newAppend();

    for (T value : partitionValues) {
      URI partitionDir = dataDir.resolve(value.toString() + "/");
      for (int fileNum = 0; fileNum < dataFilesPerPartition; fileNum++) {
        OutputFile parquetFile =
            getUniqueNumberedFilename(partitionDir.resolve(value + "-%02d.parquet").toString());
        appendFiles.appendFile(writeDataFile(parquetFile, value, recordGenerator, rowsPerDataFile));
      }
    }

    appendFiles.commit();

    return this;
  }

  public IcebergTableGenerator append(
      RecordGenerator<Void> recordGenerator, int numDataFiles, int rowsPerDataFile)
      throws IOException {
    Preconditions.checkState(table != null, "create must be called first");
    URI dataDir = getDataDirectory(id);
    AppendFiles appendFiles = getTransaction().newAppend();

    for (int fileNum = 0; fileNum < numDataFiles; fileNum++) {
      OutputFile parquetFile =
          getUniqueNumberedFilename(dataDir.resolve("%02d.parquet").toString());
      appendFiles.appendFile(
          writeUnpartitionedDataFile(parquetFile, recordGenerator, rowsPerDataFile));
    }

    appendFiles.commit();

    return this;
  }

  public IcebergTableGenerator positionalDelete(Predicate<Record> deletePredicate)
      throws IOException {
    return positionalDelete(null, deletePredicate, 0, 0, null);
  }

  public <T> IcebergTableGenerator positionalDelete(
      List<T> partitionValues, Predicate<Record> deletePredicate) throws IOException {
    return positionalDelete(partitionValues, deletePredicate, 0, 0, null);
  }

  public <T> IcebergTableGenerator positionalDelete(
      List<T> partitionValues,
      Predicate<Record> deletePredicate,
      int extraDataFileCountPerPartition,
      int extraDeleteCountPerDataFile,
      GenericRecord fakeRecord)
      throws IOException {
    URI dataDir = getDataDirectory(id);
    Expression expr =
        partitionValues != null
            ? Expressions.in(table.spec().fields().get(0).name(), partitionValues.toArray())
            : Expressions.alwaysTrue();
    CloseableIterable<FileScanTask> scanTasks = table.newScan().filter(expr).planFiles();

    RowDelta rowDelta = getTransaction().newRowDelta();

    Map<PartitionKey, List<FileScanTask>> orderedTasks =
        orderFileScanTasksByPartitionAndPath(scanTasks);
    for (PartitionKey key : orderedTasks.keySet()) {
      OutputFile deleteFile;
      String fakePathPrefix;
      if (key.size() > 0) {
        String partitionString = partitionKeyDirectoryName(key);
        URI partitionDir =
            partitionString.length() > 0 ? dataDir.resolve(partitionString + "/") : dataDir;
        deleteFile =
            getUniqueNumberedFilename(
                partitionDir + "/delete-" + partitionString + "-%02d.parquet");
        fakePathPrefix = partitionDir + partitionString + "-";
      } else {
        deleteFile = getUniqueNumberedFilename(dataDir + "/delete-%02d.parquet");
        fakePathPrefix = dataDir.toString();
      }

      Set<String> realDataFiles =
          orderedTasks.get(key).stream()
              .map(t -> t.file().path().toString())
              .collect(Collectors.toSet());
      List<String> dataFiles = new ArrayList<>(realDataFiles);
      for (int i = 0; i < extraDataFileCountPerPartition; i++) {
        String fakePath =
            fakePathPrefix + String.format("%010d-%s-fake.parquet", i, UUID.randomUUID());
        dataFiles.add(fakePath);
      }
      dataFiles.sort(String::compareTo);

      PositionDeleteWriter<Record> deleteWriter =
          Parquet.writeDeletes(deleteFile)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .overwrite()
              .rowSchema(table.schema())
              .withSpec(table.spec())
              .withPartition(key)
              .setAll(table.properties())
              .buildPositionWriter();
      try (PositionDeleteWriter<Record> writer = deleteWriter) {
        for (String path : dataFiles) {
          if (realDataFiles.contains(path)) {
            try (CloseableIterable<Record> reader =
                Parquet.read(table.io().newInputFile(path))
                    .project(table.schema())
                    .reuseContainers()
                    .createReaderFunc(
                        fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                    .build()) {

              int pos = 0;
              for (Record record : reader) {
                if (deletePredicate.test(record)) {
                  writer.delete(path, pos, record);
                }
                pos++;
              }
            }
          } else {
            for (int i = 0, pos = 0;
                i < extraDeleteCountPerDataFile;
                i++, pos += generator.intRange(1, 100)) {
              writer.delete(path, pos, fakeRecord);
            }
          }
        }
      }

      rowDelta.addDeletes(deleteWriter.toDeleteFile());
    }

    rowDelta.commit();
    return this;
  }

  public IcebergTableGenerator equalityDelete(
      Predicate<Record> deletePredicate, List<Integer> equalityIds) throws IOException {
    return equalityDelete(null, deletePredicate, equalityIds);
  }

  public <T> IcebergTableGenerator equalityDelete(
      List<T> partitionValues, Predicate<Record> deletePredicate, List<Integer> equalityIds)
      throws IOException {
    URI dataDir = getDataDirectory(id);
    Expression expr =
        partitionValues != null
            ? Expressions.in(table.spec().fields().get(0).name(), partitionValues.toArray())
            : Expressions.alwaysTrue();
    CloseableIterable<FileScanTask> scanTasks = table.newScan().filter(expr).planFiles();

    RowDelta rowDelta = getTransaction().newRowDelta();

    Map<PartitionKey, List<FileScanTask>> orderedTasks =
        orderFileScanTasksByPartitionAndPath(scanTasks);
    for (PartitionKey key : orderedTasks.keySet()) {
      OutputFile deleteFile;
      if (key.size() > 0) {
        String partitionString = partitionKeyDirectoryName(key);
        URI partitionDir =
            partitionString.length() > 0 ? dataDir.resolve(partitionString + "/") : dataDir;
        deleteFile =
            getUniqueNumberedFilename(
                partitionDir + "/eqdelete-" + partitionString + "-%02d.parquet");
      } else {
        deleteFile = getUniqueNumberedFilename(dataDir + "/eqdelete-%02d.parquet");
      }

      List<String> dataFiles =
          orderedTasks.get(key).stream()
              .map(t -> t.file().path().toString())
              .sorted(String::compareTo)
              .collect(Collectors.toList());

      EqualityDeleteWriter<Record> deleteWriter =
          Parquet.writeDeletes(deleteFile)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .overwrite()
              .rowSchema(table.schema())
              .withSpec(table.spec())
              .withPartition(key)
              .equalityFieldIds(equalityIds)
              .setAll(table.properties())
              .buildEqualityWriter();
      try (EqualityDeleteWriter<Record> writer = deleteWriter) {
        for (String path : dataFiles) {
          try (CloseableIterable<Record> reader =
              Parquet.read(table.io().newInputFile(path))
                  .project(table.schema())
                  .reuseContainers()
                  .createReaderFunc(
                      fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                  .build()) {

            for (Record record : reader) {
              if (deletePredicate.test(record)) {
                writer.delete(record);
              }
            }
          }
        }
      }

      rowDelta.addDeletes(deleteWriter.toDeleteFile());
    }

    rowDelta.commit();
    return this;
  }

  public Transaction getTransaction() {
    if (transaction == null) {
      transaction = table.newTransaction();
    }

    return transaction;
  }

  public IcebergTableGenerator commit() {
    transaction.commitTransaction();
    transaction = null;
    return this;
  }

  private URI getDataDirectory(TableIdentifier id) {
    URI uri = URI.create(warehousePath + "/");
    return uri.resolve(id.toString().replace(".", "/") + "/data/");
  }

  private OutputFile getUniqueNumberedFilename(String template) {
    int fileNum = 0;
    OutputFile name;
    do {
      name = table.io().newOutputFile(String.format(template, fileNum));
      fileNum++;
    } while (name.toInputFile().exists());

    return name;
  }

  private <T> DataFile writeDataFile(
      OutputFile parquetFile, T partitionValue, RecordGenerator<T> recordGenerator, int nrows)
      throws IOException {
    try (FileAppender<GenericRecord> appender =
        Parquet.write(parquetFile)
            .writerVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
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
          .withInputFile(parquetFile.toInputFile())
          .withMetrics(appender.metrics())
          .withFormat(FileFormat.PARQUET)
          .build();
    }
  }

  private DataFile writeUnpartitionedDataFile(
      OutputFile parquetFile, RecordGenerator<Void> recordGenerator, int nrows) throws IOException {
    try (FileAppender<GenericRecord> appender =
        Parquet.write(parquetFile)
            .writerVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .setAll(table.properties())
            .build()) {
      Stream<GenericRecord> stream =
          Stream.iterate(0, i -> i + 1)
              .limit(nrows)
              .map(i -> recordGenerator.next(generator, null));
      appender.addAll(stream.iterator());
      appender.close();

      return DataFiles.builder(table.spec())
          .withInputFile(parquetFile.toInputFile())
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
      map.get(key).sort(Comparator.comparing(t -> t.file().path().toString()));
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
