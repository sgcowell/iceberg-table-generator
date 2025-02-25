/* (C)2025 */
import com.google.common.base.Preconditions;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class ValueGenerator {

    private final int seed;
    private final Random random;
    private Iterator<Integer> idIterator;

    public ValueGenerator(int seed) {
        this.seed = seed;
        this.random = new Random(seed);
        this.idIterator = Stream.iterate(0, i -> i + 1).iterator();
    }

    public void reset() {
        random.setSeed(seed);
        idIterator = Stream.iterate(0, i -> i + 1).iterator();
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

    public LocalDate date(int year) {
        return timestamp(year).toLocalDate();
    }

    public int intRange(int min, int max) {
        Preconditions.checkArgument(max > min, "max must be > min");
        return min + random.nextInt(max - min);
    }

    public double doubleRange(double min, double max) {
        Preconditions.checkArgument(max > min, "max must be > min");
        return min + (random.nextDouble() * (max - min));
    }

    public char charRange(char min, char max) {
        Preconditions.checkArgument(max > min, "max must be > min");
        return (char) (min + random.nextInt(max - min));
    }

    public String stringRange(String min, String max, int len) {
        StringBuilder builder = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char cmin = i < min.length() ? min.charAt(i) : 'a';
            char cmax = i < max.length() ? max.charAt(i) : 'z';
            builder.append(charRange(cmin, cmax));
        }

        return builder.toString();
    }
}
