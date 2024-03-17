package xyz.a5s7.file;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.IOResult;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SubFlow;
import akka.util.ByteString;
import lombok.extern.slf4j.Slf4j;
import xyz.a5s7.model.Measurement;

import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class MeasurementFileWriter {
    private static final int MAX_SUBSTREAMS = 100;
    private static final int PARALLELISM = 1;
    private final int DEFAULT_SZ = 4;
    private int chunkSize = DEFAULT_SZ;
    private final ActorSystem system;
    private final Function<Measurement, String> filenameGenerator;
    private final Function<Measurement, String> measurementToLine;

    public MeasurementFileWriter(ActorSystem system, int chunkSize,
                                 Function<Measurement, String> filenameGenerator,
                                 Function<Measurement, String> measurementToLine) {
        this.system = system;
        this.chunkSize = chunkSize;
        this.filenameGenerator = filenameGenerator;
        this.measurementToLine = measurementToLine;
    }

    public MeasurementFileWriter(ActorSystem system, Function<Measurement, String> filenameGenerator,
                                 Function<Measurement, String> measurementToLine) {
        this.system = system;
        this.filenameGenerator = filenameGenerator;
        this.measurementToLine = measurementToLine;
    }

    /**
     * Sink that writes measurements to files partitioned by the partition field.
     * @return an Akka Stream Sink
     */
    public Sink<Measurement, CompletionStage<List<IOResult>>> partitionedFileSink() {
        return groupIntoChunks(chunkSize)
                .mapAsync(PARALLELISM, pair -> {
                    String filename = pair.first();
                    log.info("Writing {} lines to file: {}", pair.second().size(), filename);
                    return Source.from(pair.second())
                            .runWith(FileIO.toPath(Paths.get(filename), EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.APPEND)), system);
                })
                .mergeSubstreams()
                .toMat(Sink.seq(), Keep.right());
    }

    private SubFlow<Measurement, Pair<String, List<ByteString>>, NotUsed> groupIntoChunks(int chunkSize) {
        return Flow.of(Measurement.class)
                .groupBy(MAX_SUBSTREAMS, Measurement::getPartition)
                .grouped(chunkSize)
                .map(chunk -> {
                    if (chunk.isEmpty()) {
                        return new Pair<String, List<ByteString>>("", List.of());
                    }
                    return Pair.create(filenameGenerator.apply(chunk.get(0)), writeChunk(chunk));
                })
                .async();
    }

    private List<ByteString> writeChunk(List<Measurement> chunk) {
        return chunk.stream()
                .map(it -> ByteString.fromString(measurementToLine.apply(it) + "\n"))
                .collect(Collectors.toList());
    }
}
