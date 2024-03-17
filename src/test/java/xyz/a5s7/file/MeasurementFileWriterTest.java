package xyz.a5s7.file;

import akka.actor.ActorSystem;
import akka.stream.IOResult;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import xyz.a5s7.model.Measurement;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static java.nio.file.Files.readAllLines;
import static org.assertj.core.api.Assertions.assertThat;

class MeasurementFileWriterTest {
    private static final String SRC_TEST_RESOURCES = "src/test/resources/";
    private static ActorSystem system;
    private static MeasurementFileWriter fileWriter;

    @BeforeAll
    static void beforeAll() {
        system = ActorSystem.create();
        fileWriter = new MeasurementFileWriter(system,
                m -> SRC_TEST_RESOURCES + "partition-" + m.getPartition() + ".csv",
                Measurement::toString);
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system);
        // remove files
        Arrays.stream(Paths.get(SRC_TEST_RESOURCES).toFile().listFiles())
                .forEach(f -> {
                    if (f.getName().startsWith("partition-")) {
                        f.delete();
                    }
                });
    }

    @Test
    void partitionedFileSinkShouldWriteMeasurementsToFile() throws Exception {
        List<Measurement> measurements = Arrays.asList(
                new Measurement<>(1505233687023L, UUID.fromString("3c8f3f69-f084-4a0d-b0a7-ea183fabceef"), 2, 19, true),
                new Measurement<>(1505233687036L, UUID.fromString("ead3d58b-85f3-4f54-8e1e-b0a21ae99a0d"), 1, 20, true),
                new Measurement<>(1505233687037L, UUID.fromString("fe52fa24-4527-4dfd-be87-348812e0c736"), 4, 16, true)
        );
        Sink<Measurement, CompletionStage<List<IOResult>>> sinkUnderTest = fileWriter.partitionedFileSink();

        CompletionStage<List<IOResult>> future = Source.from(measurements).runWith(sinkUnderTest, system);
        List<IOResult> result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);

        assertFileContains("partition-1.csv", new String[]{measurements.get(1).toString()});
        assertFileContains("partition-2.csv", new String[]{measurements.get(0).toString()});
        assertFileContains("partition-4.csv", new String[]{measurements.get(2).toString()});

        long partitionFiles = Arrays.stream(Paths.get(SRC_TEST_RESOURCES).toFile().listFiles()).
                filter(f -> f.getName().startsWith("partition-")).count();
        assertThat(partitionFiles).isEqualTo(3);
        assertThat(result).hasSize(3);
    }

    private static void assertFileContains(final String fileName, final String[] strings) throws IOException {
        List<String> file1 = readAllLines(Paths.get(SRC_TEST_RESOURCES + fileName));
        assertThat(file1).containsExactly(strings);
    }
}