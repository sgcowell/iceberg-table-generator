import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

public class Main {

  @Parameter(
      names = {"--warehouse"},
      description = "Warehouse path")
  private String warehousePath = Paths.get(System.getenv("HOME"), "warehouse").toString();

  @Parameter(
      names = {"--conf"},
      description = "Additional hadoop configuration key/value pairs")
  private List<String> hadoopConf = ImmutableList.of();

  private Configuration conf;

  private static final Schema ORDERS_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "order_id", Types.IntegerType.get()),
          Types.NestedField.required(2, "order_year", Types.IntegerType.get()),
          Types.NestedField.required(3, "order_date", Types.TimestampType.withoutZone()),
          Types.NestedField.required(4, "source_id", Types.IntegerType.get()),
          Types.NestedField.required(5, "product_name", Types.StringType.get()),
          Types.NestedField.required(6, "amount", Types.DoubleType.get()));

  private static final List<String> PRODUCT_NAMES = ImmutableList.of("Widget", "Gizmo", "Gadget");

  private static final Schema PRODUCTS_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "product_id", Types.IntegerType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()),
          Types.NestedField.required(3, "category", Types.StringType.get()),
          Types.NestedField.required(4, "color", Types.StringType.get()),
          Types.NestedField.required(5, "created_date", Types.DateType.get()),
          Types.NestedField.required(6, "weight", Types.DoubleType.get()),
          Types.NestedField.required(7, "quantity", Types.IntegerType.get()));

  private static final List<String> PRODUCT_NAME_TEMPLATES =
      ImmutableList.of(
          "Core%s",
          "%sPress", "%sLab", "Ever%s", "%sScope", "%sKit", "%sTron", "%sView", "%sBuddy",
          "Home%s");

  private static final List<String> PRODUCT_SUFFIXES =
      ImmutableList.of("", "", "Advanced", "1000", "2000", "Deluxe", "Express", "Ultimate");

  private static final List<String> COLORS =
      ImmutableList.of(
          "black", "white", "red", "orange", "yellow", "green", "blue", "purple", "brown", "gray");

  public static void main(String[] args) {
    try {
      Main main = new Main();
      JCommander.newBuilder().addObject(main).build().parse(args);

      main.initHadoopConfFromArgs();
      main.run();
    } catch (Exception ex) {
      ExceptionUtils.printRootCauseStackTrace(ex, System.err);
    }
  }

  private void initHadoopConfFromArgs() {
    conf = new Configuration();
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.connection.ssl.enabled", "false");
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    conf.set("google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");

    for (String arg : hadoopConf) {
      String[] kv = arg.split("=");
      if (kv.length != 2) {
        System.err.format("Invalid --conf option: %s", arg);
        System.exit(1);
      }

      conf.set(kv[0], kv[1]);
    }
  }

  private void run() throws IOException {
    createSmallOrders();
    createSmallOrdersWithDeletes();
    createMultiRowGroupOrdersWithDeletes();
    createUnpartitionedOrdersWithDeletes();
    createProductsWithEqDeletes();

    //    createProductsWithEqDeletesSchemaChange();
    //    createSmallOrdersWithLargeDeleteFile();
    //    createSmallOrdersWithPartitionEvolution();
  }

  private void createSmallOrders() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(warehousePath, conf, TableIdentifier.of("orders"));
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
        new IcebergTableGenerator(warehousePath, conf, TableIdentifier.of("orders_with_deletes"));
    tableGenerator
        .create(
            ORDERS_SCHEMA, PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
        .append(ImmutableList.of(2019, 2020), this::generateOrdersRecord, 2, 100)
        .commit()
        .positionalDelete(ImmutableList.of(2019, 2020), r -> r.get(0, Integer.class) % 10 == 0)
        .commit()
        .append(ImmutableList.of(2020, 2021), this::generateOrdersRecord, 2, 100)
        .commit()
        .positionalDelete(ImmutableList.of(2019, 2020), r -> r.get(0, Integer.class) % 10 == 3)
        .commit()
        .positionalDelete(ImmutableList.of(2021), r -> r.get(0, Integer.class) % 10 == 6)
        .commit();
  }

  private void createMultiRowGroupOrdersWithDeletes() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(
            warehousePath, conf, TableIdentifier.of("multi_rowgroup_orders_with_deletes"));
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
        .positionalDelete(ImmutableList.of(2021), r -> r.get(0, Integer.class) % 10 < 3)
        .commit()
        .positionalDelete(
            ImmutableList.of(2021),
            r -> r.get(0, Integer.class) % 10 > 0 && r.get(0, Integer.class) % 100 == 5)
        .commit()
        .positionalDelete(
            ImmutableList.of(2020, 2021),
            r -> r.get(0, Integer.class) % 3000 >= 700 && r.get(0, Integer.class) % 3000 < 1200)
        .commit();
  }

  private void createSmallOrdersWithLargeDeleteFile() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(
            warehousePath, conf, TableIdentifier.of("orders_with_large_delete_file"));
    tableGenerator
        .create(
            ORDERS_SCHEMA, PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
        .append(ImmutableList.of(2021), this::generateOrdersRecord, 2, 100)
        .commit()
        .positionalDelete(
            ImmutableList.of(2021),
            r -> r.get(0, Integer.class) % 10 < 3,
            10000,
            10000,
            getFakeOrdersRecordForExtraDeletes());
  }

  private void createSmallOrdersWithPartitionEvolution() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(warehousePath, conf, TableIdentifier.of("orders_part_evol"));
    tableGenerator
        .create(
            ORDERS_SCHEMA, PartitionSpec.builderFor(ORDERS_SCHEMA).identity("order_year").build())
        .append(ImmutableList.of(2019, 2020), this::generateOrdersRecord, 2, 100)
        .commit()
        .append(ImmutableList.of(2021), this::generateOrdersRecord, 2, 100)
        .commit()
        .updateSpec(
            ImmutableList.of(Expressions.ref("source_id")),
            ImmutableList.of(Expressions.ref("order_year")))
        .commit()
        .append(
            ImmutableList.of(0, 1, 2, 3, 4), this::generateOrdersRecordWithSourceIdPartition, 1, 40)
        .commit();
  }

  private void createUnpartitionedOrdersWithDeletes() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(
            warehousePath, conf, TableIdentifier.of("unpartitioned_orders_with_deletes"));
    tableGenerator
        .create(ORDERS_SCHEMA, PartitionSpec.unpartitioned())
        .append(this::generateUnpartitionedOrdersRecord, 2, 100)
        .commit()
        .positionalDelete(r -> r.get(0, Integer.class) % 10 == 0)
        .commit()
        .append(this::generateUnpartitionedOrdersRecord, 2, 100)
        .commit()
        .positionalDelete(r -> r.get(0, Integer.class) % 10 == 3)
        .commit();
  }

  /**
   * Creates a 'products_with_eq_deletes' table with 3 partitions on the category column: widget,
   * gadget, gizmo. 5 data files with 200 rows each are created - 2 in widget, 2 in gizmo, 1 in
   * gadget. Each data file has two row groups, each with 100 rows.
   *
   * <p>
   *
   * <p>Creation steps:
   *
   * <p>
   *
   * <pre>
   * 1. Insert 200 rows with category 'widget', product_ids from [ 0 .. 199 ].            Total rows: 200
   * 2. Delete product_ids [ 0 .. 29 ] via equality delete on product_id.                 Total rows: 170
   * 3. Insert 200 rows with category 'gizmo', product_ids from [ 200 .. 399 ].           Total rows: 370
   * 4. Delete all products with color 'green' via equality delete on color.              Total rows: 333
   * 5. Insert 600 rows, 200 each in categories 'widget', 'gizmo', 'gadget' with          Total rows: 933
   *    product_ids [ 400 .. 999]
   * 6. Delete product_ids [ 100 .. 199 ], [ 300 .. 399 ], [ 500 .. 599 ],                Total rows: 453
   *    [ 700 .. 799 ], [ 900 .. 999 ] via equality delete on product_id.
   * 7. Delete product_ids [ 50 .. 52 ] via positional delete.                            Total rows: 450
   *
   * Total rows added   : 1000
   * Total rows deleted : 550 (547 via equality delete, 3 via position delete)
   * Final row count    : 450
   * </pre>
   */
  private void createProductsWithEqDeletes() throws IOException {
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(
            warehousePath, conf, TableIdentifier.of("products_with_eq_deletes"));
    tableGenerator
        .create(
            PRODUCTS_SCHEMA,
            PartitionSpec.builderFor(PRODUCTS_SCHEMA).identity("category").build(),
            ImmutableMap.of(
                // Iceberg will write at minimum 100 rows per rowgroup, so set row group size small
                // enough to
                // guarantee that happens
                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1)))
        // add 200 rows to widget partition
        .append(ImmutableList.of("widget"), this::generateProductsRecord, 1, 200)
        .commit()
        // delete product_ids [ 0 .. 29 ] via equality delete - 30 rows removed
        .equalityDelete(
            ImmutableList.of("widget"),
            r -> r.get(0, Integer.class) < 30,
            equalityIds(PRODUCTS_SCHEMA, "product_id"))
        .commit()
        // add 200 rows to gizmo partition
        .append(ImmutableList.of("gizmo"), this::generateProductsRecord, 1, 200)
        .commit()
        // delete all products with color 'green' via equality delete - 37 rows removed
        .equalityDelete(
            ImmutableList.of("widget", "gizmo"),
            r -> r.get(3, String.class).equals("green"),
            equalityIds(PRODUCTS_SCHEMA, "color"))
        .commit()
        // add 200 rows each to widget, gadget, and gizmo partitions
        .append(ImmutableList.of("widget", "gadget", "gizmo"), this::generateProductsRecord, 1, 200)
        .commit()
        // delete product ids [ 100 .. 199 ], [ 300 .. 399 ], [ 500 .. 599 ], [ 700 .. 799 ], [ 900
        // .. 999 ]
        // taking into account previous deletions this deletes 480 rows
        .equalityDelete(
            ImmutableList.of("widget", "gadget", "gizmo"),
            r -> r.get(0, Integer.class) % 200 >= 100,
            equalityIds(PRODUCTS_SCHEMA, "product_id"))
        .commit()
        // delete product_ids [ 50 .. 52 ] via positional delete - 3 rows removed
        .positionalDelete(
            ImmutableList.of("widget"),
            r -> r.get(0, Integer.class) >= 50 && r.get(0, Integer.class) < 53)
        .commit();
  }

  private void createProductsWithEqDeletesSchemaChange() throws IOException {
    Schema initialSchema = PRODUCTS_SCHEMA.select("product_id", "name", "category");
    IcebergTableGenerator tableGenerator =
        new IcebergTableGenerator(
            warehousePath, conf, TableIdentifier.of("products_with_schema_change"));
    tableGenerator
        .create(
            initialSchema,
            PartitionSpec.builderFor(initialSchema).identity("category").build(),
            ImmutableMap.of(
                // Iceberg will write at minimum 100 rows per rowgroup, so set row group size small
                // enough to
                // guarantee that happens
                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(1)))
        // add 200 rows to widget partition
        .append(ImmutableList.of("widget"), createProductsRecordGenerator(tableGenerator), 1, 200)
        .commit()
        // delete product_ids [ 0 .. 29 ] via equality delete - 30 rows removed
        .equalityDelete(
            ImmutableList.of("widget"),
            r -> r.get(0, Integer.class) < 30,
            equalityIds(PRODUCTS_SCHEMA, "product_id"))
        .commit();

    // add color column, remove product_id column
    UpdateSchema updateSchema = tableGenerator.getTransaction().updateSchema();
    updateSchema.addColumn("color", Types.StringType.get());
    updateSchema.deleteColumn("product_id");
    updateSchema.commit();

    tableGenerator
        // add 200 rows to gizmo partition
        .append(ImmutableList.of("gizmo"), createProductsRecordGenerator(tableGenerator), 1, 200)
        .commit();

    /*
       // delete all products with color 'green' via equality delete
       .equalityDelete(ImmutableList.of("widget", "gizmo"), r -> r.get(3, String.class).equals("green"),
           equalityIds(PRODUCTS_SCHEMA, "color"))
       .commit();

    */
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

  private GenericRecord generateOrdersRecordWithSourceIdPartition(
      ValueGenerator generator, Integer partitionValue) {
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

  private GenericRecord generateProductsRecord(ValueGenerator generator, String category) {
    GenericRecord record = GenericRecord.create(PRODUCTS_SCHEMA);
    int id = generator.id();
    String name =
        String.format(generator.select(PRODUCT_NAME_TEMPLATES), StringUtils.capitalize(category));
    String suffix = generator.select(PRODUCT_SUFFIXES);
    if (!suffix.isEmpty()) {
      name = name + " " + suffix;
    }

    record.set(0, id);
    record.set(1, name);
    record.set(2, category);
    record.set(3, COLORS.get(id % COLORS.size()));
    record.set(4, LocalDate.of(2022 - (id / 12), 12 - (id % 12), 1));
    record.set(5, generator.doubleRange(0.1, 50.0));
    record.set(6, generator.intRange(0, 10000));
    return record;
  }

  private RecordGenerator<String> createProductsRecordGenerator(
      IcebergTableGenerator tableGenerator) {
    return (generator, category) -> {
      GenericRecord record = GenericRecord.create(tableGenerator.getTable().schema());
      int id = generator.id();
      String name =
          String.format(generator.select(PRODUCT_NAME_TEMPLATES), StringUtils.capitalize(category));
      String suffix = generator.select(PRODUCT_SUFFIXES);
      if (!suffix.isEmpty()) {
        name = name + " " + suffix;
      }

      for (int i = 0; i < record.size(); i++) {
        Object value = null;
        switch (record.struct().fields().get(i).name()) {
          case "product_id":
            value = id;
            break;
          case "name":
            value = name;
            break;
          case "category":
            value = category;
            break;
          case "color":
            value = COLORS.get(id % COLORS.size());
            break;
          case "created_date":
            value = LocalDate.of(2022 - (id / 12), 12 - (id % 12), 1);
            break;
          case "weight":
            value = generator.doubleRange(0.1, 50.0);
            break;
          case "quantity":
            value = generator.intRange(0, 10000);
            break;
        }

        record.set(i, value);
      }

      return record;
    };
  }

  private List<Integer> equalityIds(Schema schema, String... fields) {
    return Arrays.stream(fields)
        .map(f -> schema.findField(f).fieldId())
        .collect(Collectors.toList());
  }
}
