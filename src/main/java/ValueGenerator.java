import java.time.LocalDateTime;
import java.time.Month;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.parquet.Preconditions;

public class ValueGenerator {

  private final int seed;
  private final Random random;
  private Iterator<Integer> idIterator;

  public ValueGenerator(int seed) {
    this.seed = seed;
    this.random = new Random(seed);
    this.idIterator = Stream.iterate(1, i -> i + 1).iterator();
  }

  public void reset() {
    random.setSeed(seed);
    idIterator = Stream.iterate(1, i -> i + 1).iterator();
  }

  public int id() {
    return idIterator.next();
  }

  public <T> T select(List<T> choices) {
    return choices.get(random.nextInt(choices.size()));
  }

  public LocalDateTime timestamp() {
    return timestamp(random.nextInt(LocalDateTime.now().getYear() - 10) + 1);
  }

  public LocalDateTime timestamp(int year) {
    Month month = Month.of(random.nextInt(12) + 1);
    int ndays;
    switch (month) {
      case JANUARY:
      case MARCH:
      case MAY:
      case JULY:
      case AUGUST:
      case OCTOBER:
      case DECEMBER:
        ndays = 31;
        break;
      case FEBRUARY:
        ndays = 28;
        break;
      default:
        ndays = 30;
        break;
    }
    return LocalDateTime.of(
        year,
        month,
        random.nextInt(ndays) + 1,
        random.nextInt(23),
        random.nextInt(60),
        random.nextInt(60));
  }

  public int intRange(int min, int max) {
    Preconditions.checkArgument(max > min, "max must be > min");
    return min + random.nextInt(max - min);
  }

  public double doubleRange(double min, double max) {
    Preconditions.checkArgument(max > min, "max must be > min");
    return min + (random.nextDouble() * (max - min));
  }
}