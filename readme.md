# Measurements processing

## How to build
```bash
mvn clean package
```

## How to run

Requires Java 17.
```bash
java -jar target/measures-1.0-SNAPSHOT-fat.jar input.csv
```

The program will generate out files to output folder.
The output folder will be created if it does not exist and will be cleaned if it exists.