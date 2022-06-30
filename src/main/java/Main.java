
import static org.apache.iceberg.expressions.Expressions.month;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

public class Main {

  @Parameter(
      names = {"--warehouse"},
      description = "Warehouse path")
  private String warehousePath = Paths.get(System.getenv("HOME"), "warehouse").toString();

  private static final Schema ORDERS_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "order_id", Types.IntegerType.get()),
          Types.NestedField.required(2, "order_year", Types.IntegerType.get()),
          Types.NestedField.required(3, "order_date", Types.TimestampType.withoutZone()),
          Types.NestedField.required(4, "source_id", Types.IntegerType.get()),
          Types.NestedField.required(5, "product_name", Types.StringType.get()),
          Types.NestedField.required(6, "amount", Types.DoubleType.get()));

  private static final List<String> PRODUCT_NAMES = ImmutableList.of("Widget", "Gizmo", "Gadget");

  public static void main(String[] args) {
    try {
      Main main = new Main();
      JCommander.newBuilder().addObject(main).build().parse(args);

      main.run();
    } catch (Exception ex) {
      ExceptionUtils.printRootCauseStackTrace(ex, System.err);
    }
  }

  private void run() throws IOException {
    createSmallOrders();
    createSmallOrdersWithDeletes();
    createMultiRowGroupOrdersWithDeletes();
    createUnpartitionedOrdersWithDeletes();

//    createSmallOrdersWithLargeDeleteFile();
//    createSmallOrdersWithPartitionEvolution();
  }

  private void createSmallOrders() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(warehousePath, TableIdentifier.of("orders"));
    tableGenerator
        .create(
            ORDERS_SCHEMA, PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
        .append(ImmutableList.of(2019, 2020), this::generateOrdersRecord, 2, 100)
        .commit()
        .append(ImmutableList.of(2021), this::generateOrdersRecord, 2, 100)
        .commit();
  }

  private void createSmallOrdersWithDeletes() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(warehousePath, TableIdentifier.of("orders_with_deletes"));
    tableGenerator
        .create(
            ORDERS_SCHEMA, PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
        .append(ImmutableList.of(2019, 2020), this::generateOrdersRecord, 2, 100)
        .commit()
        .mergeOnReadDelete(ImmutableList.of(2019, 2020), r -> r.get(0, Integer.class) % 10 == 0)
        .commit()
        .append(ImmutableList.of(2020, 2021), this::generateOrdersRecord, 2, 100)
        .commit()
        .mergeOnReadDelete(ImmutableList.of(2019, 2020), r -> r.get(0, Integer.class) % 10 == 3)
        .commit()
        .mergeOnReadDelete(ImmutableList.of(2021), r -> r.get(0, Integer.class) % 10 == 6)
        .commit();
  }

  private void createMultiRowGroupOrdersWithDeletes() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(warehousePath, TableIdentifier.of("multi_rowgroup_orders_with_deletes"));
    tableGenerator
        .create(
            ORDERS_SCHEMA,
            PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build(),
            ImmutableMap.of(
                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(16 * 1024),
                TableProperties.PARQUET_PAGE_SIZE_BYTES, Integer.toString(4 * 1024),
                TableProperties.PARQUET_DICT_SIZE_BYTES, Integer.toString(4 * 1024)))
        .append(ImmutableList.of(2019, 2020, 2021), this::generateOrdersRecord, 3, 1000)
        .commit()
        .mergeOnReadDelete(ImmutableList.of(2021), r -> r.get(0, Integer.class) % 10 < 3)
        .commit()
        .mergeOnReadDelete(
            ImmutableList.of(2021),
            r -> r.get(0, Integer.class) % 10 > 0 && r.get(0, Integer.class) % 100 == 5)
        .commit()
        .mergeOnReadDelete(ImmutableList.of(2020, 2021),
            r -> r.get(0, Integer.class) % 3000 >= 700 && r.get(0, Integer.class) % 3000 < 1200)
        .commit();
  }

  private void createSmallOrdersWithLargeDeleteFile() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(warehousePath, TableIdentifier.of("orders_with_large_delete_file"));
    tableGenerator
        .create(
            ORDERS_SCHEMA, PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
        .append(ImmutableList.of(2021), this::generateOrdersRecord, 2, 100)
        .commit()
        .mergeOnReadDelete(ImmutableList.of(2021), r -> r.get(0, Integer.class) % 10 < 3,
            10000, 10000, getFakeOrdersRecordForExtraDeletes());
  }

  private void createSmallOrdersWithPartitionEvolution() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(warehousePath, TableIdentifier.of("orders_part_evol"));
    tableGenerator
        .create(
            ORDERS_SCHEMA, PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
        .append(ImmutableList.of(2019, 2020), this::generateOrdersRecord, 2, 100)
        .commit()
        .append(ImmutableList.of(2021), this::generateOrdersRecord, 2, 100)
        .commit()
        .updateSpec(ImmutableList.of(Expressions.ref("source_id")), ImmutableList.of(Expressions.ref("order_year")))
        .commit()
        .append(ImmutableList.of(0, 1, 2, 3, 4), this::generateOrdersRecordWithSourceIdPartition, 1, 40)
        .commit();
  }

  private void createUnpartitionedOrdersWithDeletes() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(warehousePath, TableIdentifier.of("unpartitioned_orders_with_deletes"));
    tableGenerator
        .create(ORDERS_SCHEMA, PartitionSpec.unpartitioned())
        .append(this::generateUnpartitionedOrdersRecord, 2, 100)
        .commit()
        .mergeOnReadDelete(r -> r.get(0, Integer.class) % 10 == 0)
        .commit()
        .append(this::generateUnpartitionedOrdersRecord, 2, 100)
        .commit()
        .mergeOnReadDelete(r -> r.get(0, Integer.class) % 10 == 3)
        .commit();
  }
  
  private GenericRecord generateOrdersRecord(ValueGenerator generator, Integer partitionValue) {
    GenericRecord record = GenericRecord.create(ORDERS_SCHEMA);
    record.set(0, generator.id());
    record.set(1, partitionValue);
    record.set(2, generator.timestamp(partitionValue));
    record.set(3, generator.intRange(0, 5));
    record.set(4, generator.select(PRODUCT_NAMES) + " " + generator.intRange(0, 100));
    record.set(5, generator.doubleRange(0, 100));
    return record;
  }

  private GenericRecord generateOrdersRecordWithSourceIdPartition(ValueGenerator generator, Integer partitionValue) {
    int orderYear = generator.intRange(2019, 2022);
    GenericRecord record = GenericRecord.create(ORDERS_SCHEMA);
    record.set(0, generator.id());
    record.set(1, orderYear);
    record.set(2, generator.timestamp(orderYear));
    record.set(3, partitionValue);
    record.set(4, generator.select(PRODUCT_NAMES) + " " + generator.intRange(0, 100));
    record.set(5, generator.doubleRange(0, 100));
    return record;
  }

  private GenericRecord generateUnpartitionedOrdersRecord(ValueGenerator generator, Void unused) {
    GenericRecord record = GenericRecord.create(ORDERS_SCHEMA);
    int orderYear = generator.intRange(2019, 2022);
    record.set(0, generator.id());
    record.set(1, orderYear);
    record.set(2, generator.timestamp(orderYear));
    record.set(3, generator.intRange(0, 5));
    record.set(4, generator.select(PRODUCT_NAMES) + " " + generator.intRange(0, 100));
    record.set(5, generator.doubleRange(0, 100));
    return record;

  }

  private GenericRecord getFakeOrdersRecordForExtraDeletes() {
    GenericRecord record = GenericRecord.create(ORDERS_SCHEMA);
    record.set(0, 0);
    record.set(1, 0);
    record.set(2, LocalDateTime.now());
    record.set(3, 0);
    record.set(4, "");
    record.set(5, 0.0);
    return record;
  }
}
