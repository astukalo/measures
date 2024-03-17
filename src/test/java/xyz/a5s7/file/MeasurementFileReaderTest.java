package xyz.a5s7.file;

import akka.actor.ActorSystem;
import akka.stream.IOResult;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import xyz.a5s7.model.Measurement;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


public class MeasurementFileReaderTest {
    private static ActorSystem system;
    private static MeasurementFileReader fileReader;

    @BeforeAll
    static void beforeAll() {
        system = ActorSystem.create();
        // could be replaced with a mock
        MeasurementParser parser = new MeasurementParser(",");
        fileReader = new MeasurementFileReader(system, parser);
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system);
    }

    private static Sink<Measurement, CompletionStage<List<IOResult>>> devNullSink(List<Measurement> list) {
        return Flow.of(Measurement.class)
                .map(m -> {
                    list.add(m);
                    return IOResult.createSuccessful(1);
                })
                .toMat(Sink.seq(), Keep.right());
    }

    @Test
    public void processFileShouldIgnoreInvalidMeasurements() throws Exception {
        String filePath = "src/test/resources/mixed_measurements.csv";
        List<Measurement> list = new ArrayList<>();
        Sink<Measurement, CompletionStage<List<IOResult>>> sink = devNullSink(list);

        CompletionStage<List<IOResult>> result = fileReader.processFile(Paths.get(filePath), sink);

        List<IOResult> ioResults = result.toCompletableFuture().get(3, TimeUnit.SECONDS);

        assertThat(list).hasSize(4).containsExactly(
                new Measurement<>(1505233687023L, UUID.fromString("3c8f3f69-f084-4a0d-b0a7-ea183fabceef"), 2, 19, true),
                new Measurement<>(1505233687036L, UUID.fromString("ead3d58b-85f3-4f54-8e1e-b0a21ae99a0d"), 1, 20, true),
                new Measurement<>(1505233687037L, UUID.fromString("345f7eb1-bf33-40c1-82a4-2f91c658803f"), 3, 55, true),
                new Measurement<>(1505233687037L, UUID.fromString("fe52fa24-4527-4dfd-be87-348812e0c736"), 4, 16, true)
        );
        assertThat(ioResults).hasSize(4);
    }
}