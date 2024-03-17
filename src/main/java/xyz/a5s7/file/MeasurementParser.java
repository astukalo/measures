package xyz.a5s7.file;

import xyz.a5s7.model.Measurement;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;


public class MeasurementParser {
    private static final int MIN_LENGTH = 3;
    private final String delimiter;

    public MeasurementParser(String delimiter) {
        this.delimiter = delimiter;
    }

    public Measurement<Integer> parse(String csvLine) {
        var rawValues = csvLine.split(delimiter);
        if (rawValues.length <= MIN_LENGTH) {
            return new Measurement<>();
        }

        //here we can decide what type of measurement we want to return
        var measurement = new Measurement<Integer>();
        try {
            measurement.setTimestamp(parseTimestamp(rawValues[0]));
            measurement.setPartition(parsePartition(rawValues[1]));
            measurement.setUuid(parseUUID(rawValues[2]));
        } catch (Exception e) {
            //we can collect errors here and put them in the measurement object
            return measurement;
        }

        List<Integer> integers = IntStream.range(MIN_LENGTH, rawValues.length)
                .mapToObj(i -> toInt(rawValues[i]))
                .filter(Objects::nonNull)
                .toList();

        measurement.setData(integers.stream().reduce(Integer::sum).orElse(0));

        int hashTagsCount = rawValues.length - MIN_LENGTH;
        measurement.setValid(integers.size() == hashTagsCount);
        return measurement;
    }

    private static UUID parseUUID(String rawValue) {
        return UUID.fromString(rawValue);
    }

    private static int parsePartition(String s) {
        var partition = Integer.parseInt(s);

        if (partition < 1 || partition > 4) {
            throw new IllegalArgumentException("Partition must be between 1 and 4");
        }
        return partition;
    }

    private static long parseTimestamp(String s) {
        return Long.parseLong(s);
    }

    private static Integer toInt(String s) {
        return switch (s) {
            case "#one" -> 1;
            case "#two" -> 2;
            case "#three" -> 3;
            case "#four" -> 4;
            case "#five" -> 5;
            case "#six" -> 6;
            case "#seven" -> 7;
            case "#eight" -> 8;
            case "#nine" -> 9;
            case "#ten" -> 10;
            default -> null;
        };
    }
}
