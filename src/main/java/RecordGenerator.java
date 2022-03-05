import org.apache.iceberg.data.GenericRecord;

public interface RecordGenerator<T> {
  GenericRecord next(ValueGenerator generator, T partitionValue);
}
