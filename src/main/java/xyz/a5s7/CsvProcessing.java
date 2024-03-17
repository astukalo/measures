package xyz.a5s7;

import akka.actor.ActorSystem;
import lombok.extern.slf4j.Slf4j;
import xyz.a5s7.file.MeasurementFileWriter;
import xyz.a5s7.file.MeasurementFileReader;
import xyz.a5s7.file.MeasurementParser;
import xyz.a5s7.model.Measurement;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

@Slf4j
public class CsvProcessing {
    private static final String OUTPUT_DIR = "output";
    private static final String DELIMITER = ",";

    public static void main(String[] args) {
        if (args.length != 1) {
            log.error("Usage: CsvProcessing <input-file>");
            System.exit(1);
        }

        cleanUp();

        ActorSystem actorSystem = ActorSystem.create("csvProcessingSystem");
        MeasurementParser measurementParser = new MeasurementParser(DELIMITER);
        var measurementFileReader = new MeasurementFileReader(actorSystem, measurementParser);
        var measurementFileWriter = new MeasurementFileWriter(actorSystem,
                CsvProcessing::outputFilename,
                CsvProcessing::toCsvLine
        );

        var fileName = args[0];

        measurementFileReader
                .processFile(Paths.get(fileName), measurementFileWriter.partitionedFileSink())
                .thenAccept(done -> {
                    log.info("File processing completed");
                    actorSystem.terminate();
                })
        ;
    }

    private static void cleanUp() {
        Path outDir = Paths.get(OUTPUT_DIR);
        removeFiles(outDir);
        outDir.toFile().mkdirs();
    }

    private static void removeFiles(Path outDir) {
        try {
            if (Files.exists(outDir)) {
                Files.walk(outDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                log.error("Error deleting file or directory: " + path, e);
                            }
                        });
            }
        } catch (IOException e) {
            log.error("Error walking directory: " + outDir, e);
        }
    }

    private static String toCsvLine(Measurement<?> m) {
        return m.getTimestamp() + DELIMITER + m.getPartition() + DELIMITER + m.getUuid() + DELIMITER + m.getData();
    }

    private static String outputFilename(Measurement<?> measurement) {
        return OUTPUT_DIR + "/" + "output-file-" + measurement.getPartition() + ".csv";
    }
}