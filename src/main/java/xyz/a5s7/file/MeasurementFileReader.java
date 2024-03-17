package xyz.a5s7.file;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.IOResult;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.FramingTruncation;
import akka.stream.javadsl.Sink;
import akka.util.ByteString;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import xyz.a5s7.model.Measurement;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletionStage;

@Slf4j
@RequiredArgsConstructor
public class MeasurementFileReader {
    private final ActorSystem system;
    private final MeasurementParser measurementParser;

    /**
     * Process a file and write the output to a sink.
     *
     * @param filePath the path to the file to process
     * @param output   the sink to write the output to
     * @return a completion stage with results of writing to the sink
     */
    public CompletionStage<List<IOResult>> processFile(Path filePath, final Sink<Measurement, CompletionStage<List<IOResult>>> output) {
        return FileIO.fromPath(filePath)
                .via(fileReadingFlow())
                .via(parsingFlow(measurementParser))
                .divertTo(errorSink(), m -> !m.isValid())
                .runWith(output, system);
    }

    Sink<Measurement, NotUsed> errorSink() {
        return Flow.of(Measurement.class)
                .map(Measurement::toString)
                //TODO need raw line here
                .to(Sink.foreach(it -> log.error("Invalid measurement: {}", it)));
    }

    Flow<ByteString, String, NotUsed> fileReadingFlow() {
        return Flow.of(ByteString.class)
                .via(Framing.delimiter(ByteString.fromString("\n"), 256, FramingTruncation.ALLOW))
                .map(ByteString::utf8String)
                .filterNot(String::isEmpty);
    }

    Flow<String, Measurement, NotUsed> parsingFlow(MeasurementParser measurementParser) {
        return Flow.of(String.class).map(measurementParser::parse);
    }

}
