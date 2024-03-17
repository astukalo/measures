package xyz.a5s7.file;

import org.junit.jupiter.api.Test;
import xyz.a5s7.model.Measurement;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MeasurementParserTest {
    MeasurementParser parser = new MeasurementParser(",");

    @Test
    void parseShouldReturnMeasurementWithCorrectContent() {
        Measurement<Integer> measurement = parser.parse("123,1,550e8400-e29b-41d4-a716-446655440000,#one,#two,#three");

        assertThat(measurement.getTimestamp()).isEqualTo(123);
        assertThat(measurement.getPartition()).isEqualTo(1);
        assertThat(measurement.getUuid()).isEqualTo(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertThat(measurement.getData()).isEqualTo(6); // Sum of 1, 2, 3
        assertThat(measurement.isValid()).isTrue();
    }

    @Test
    void parseShouldReturnInvalidMeasurementWhenDataIsIncorrect() {
        Measurement<Integer> measurement = parser.parse("123,1,550e8400-e29b-41d4-a716-446655440000,#one,#eleven,#three");

        assertThat(measurement.getTimestamp()).isEqualTo(123);
        assertThat(measurement.getPartition()).isEqualTo(1);
        assertThat(measurement.getUuid()).isEqualTo(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertThat(measurement.getData()).isEqualTo(4); // Sum of 1, 3
        assertThat(measurement.isValid()).isFalse();
    }

    @Test
    void parseShouldReturnInvalidMeasurementWhenPartitionIsOutOfRange() {
        Measurement<Integer> measurement = parser.parse("123,5,550e8400-e29b-41d4-a716-446655440000,#one");
        assertThat(measurement.isValid()).isFalse();
    }

    @Test
    void parseShouldReturnInvalidMeasurementWhenUUIDIsIncorrect() {
        Measurement<Integer> measurement = parser.parse("123,5,550e8400-e29b,#one");
        assertThat(measurement.isValid()).isFalse();
    }

    @Test
    void parseShouldReturnInvalidMeasurementWhenLineIsShort() {
        Measurement<Integer> measurement = parser.parse("123,1,550e8400-e29b-41d4-a716-446655440000");
        assertThat(measurement.isValid()).isFalse();
    }

}
