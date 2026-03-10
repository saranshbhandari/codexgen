package com.test.dataflowengine.processors.taskprocessors.datataskprocessorhelpers.datareaders;

import com.test.dataflowengine.models.enums.FileDelimiter;
import com.test.dataflowengine.models.tasksettings.DataTaskSettings;
import com.test.dataflowengine.models.tasksettings.subsettings.FileSettings;
import com.test.dataflowengine.processors.taskprocessors.datataskprocessorhelpers.Contracts.DataReader;
import com.test.dataflowengine.utils.InvalidUtf8FileException;
import com.test.dataflowengine.utils.Utf8ValidationUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class CsvFileReader implements DataReader {

    private final FileSettings fs;

    private BufferedReader reader;
    private CSVParser parser;
    private Iterator<CSVRecord> iterator;

    private List<String> headers = new ArrayList<>();
    private Map<String, Object> bufferedFirstRow;

    public CsvFileReader(DataTaskSettings settings) {
        this.fs = settings.getSource().getFileSettings();
    }

    @Override
    public void open() throws Exception {
        log.info("[CsvFileReader] Opening CSV reader. path={}", fs.getFilePath());

        validateSettings();

        try {
            Utf8ValidationUtils.validateUtf8File(fs.getFilePath());
        } catch (InvalidUtf8FileException e) {
            log.error("[CsvFileReader] Invalid UTF-8 file. path={}, error={}", fs.getFilePath(), e.getMessage());
            throw e;
        }

        char delimiter = resolveDelimiterChar(fs);

        Path path = Path.of(fs.getFilePath());
        this.reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);

        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter)
                .setQuote('"')
                .setIgnoreEmptyLines(true)
                .setTrim(false)
                .get();

        this.parser = format.parse(reader);
        this.iterator = parser.iterator();

        this.headers.clear();
        this.bufferedFirstRow = null;

        if (!iterator.hasNext()) {
            log.warn("[CsvFileReader] CSV file is empty. path={}", fs.getFilePath());
            return;
        }

        CSVRecord firstRecord;
        try {
            firstRecord = iterator.next();
        } catch (Exception e) {
            log.error("[CsvFileReader] Failed to read first record. path={}", fs.getFilePath(), e);
            throw e;
        }

        if (Boolean.TRUE.equals(fs.isFirstRowColumn())) {
            this.headers = normalizeHeaders(recordToList(firstRecord));
            log.info("[CsvFileReader] Header row detected. columnCount={}, headers={}", headers.size(), headers);
        } else {
            this.headers = generateHeaders(firstRecord.size());
            this.bufferedFirstRow = rowFromRecord(firstRecord);
            log.info("[CsvFileReader] No header row present. Generated columnCount={}", headers.size());
        }

        log.info("[CsvFileReader] CSV reader opened successfully. path={}, delimiter='{}', columnCount={}",
                fs.getFilePath(), printableDelimiter(delimiter), headers.size());
    }

    @Override
    public List<Map<String, Object>> readBatch(int batchSize) throws Exception {
        log.debug("[CsvFileReader] Reading batch. batchSize={}", batchSize);

        if (batchSize <= 0) {
            batchSize = 1000;
        }

        if (iterator == null) {
            return new ArrayList<>();
        }

        List<Map<String, Object>> batch = new ArrayList<>(Math.min(batchSize, 2048));

        if (bufferedFirstRow != null) {
            batch.add(bufferedFirstRow);
            bufferedFirstRow = null;

            if (batch.size() >= batchSize) {
                return batch;
            }
        }

        while (batch.size() < batchSize && iterator.hasNext()) {
            CSVRecord record = null;

            try {
                record = iterator.next();
                batch.add(rowFromRecord(record));
            } catch (Exception e) {
                if (record != null) {
                    log.error("[CsvFileReader] Faulty CSV row detected. path={}, recordNumber={}, charPosition={}, row={}",
                            fs.getFilePath(),
                            record.getRecordNumber(),
                            record.getCharacterPosition(),
                            safeRecordToString(record),
                            e);
                } else {
                    log.error("[CsvFileReader] CSV parsing failure before record creation. path={}",
                            fs.getFilePath(), e);
                }
                throw e;
            }
        }

        if (batch.isEmpty()) {
            return null;
        }

        log.debug("[CsvFileReader] Batch read complete. rowsRead={}", batch.size());
        return batch;
    }

    @Override
    public void close() {
        log.info("[CsvFileReader] Closing CSV reader. path={}", fs != null ? fs.getFilePath() : null);

        if (parser != null) {
            try {
                parser.close();
            } catch (Exception e) {
                log.error("[CsvFileReader] Error closing parser", e);
            }
            parser = null;
        }

        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                log.error("[CsvFileReader] Error closing reader", e);
            }
            reader = null;
        }

        iterator = null;
    }

    private void validateSettings() {
        log.info("[CsvFileReader] Validating settings. path={}", fs != null ? fs.getFilePath() : null);

        if (fs == null) {
            throw new IllegalArgumentException("FileSettings is null");
        }

        if (fs.getFilePath() == null || fs.getFilePath().trim().isEmpty()) {
            throw new IllegalArgumentException("filePath is required");
        }

        if (fs.getFileType() == null || !"csv".equalsIgnoreCase(fs.getFileType().trim())) {
            throw new IllegalArgumentException("fileType must be 'csv'");
        }

        if (fs.getFileDelimiter() == null) {
            throw new IllegalArgumentException("fileDelimiter is required");
        }

        if (fs.getFileDelimiter() == FileDelimiter.CUSTOM) {
            if (fs.getCustomDelimiter() == null || fs.getCustomDelimiter().isEmpty()) {
                throw new IllegalArgumentException("customDelimiter is required when fileDelimiter=CUSTOM");
            }
        }
    }

    private static char resolveDelimiterChar(FileSettings fs) {
        FileDelimiter delimiter = fs.getFileDelimiter();

        switch (delimiter) {
            case COMMA:
                return ',';
            case TAB:
                return '\t';
            case PIPE:
                return '|';
            case CUSTOM:
                String customDelimiter = fs.getCustomDelimiter();
                if (customDelimiter.length() != 1) {
                    throw new IllegalArgumentException(
                            "Apache Commons CSV supports only a single character delimiter. customDelimiter=" + customDelimiter
                    );
                }
                return customDelimiter.charAt(0);
            default:
                return ',';
        }
    }

    private Map<String, Object> rowFromRecord(CSVRecord record) {
        if (record.size() > headers.size()) {
            extendHeadersTo(record.size());
            log.warn("[CsvFileReader] Row wider than header. recordNumber={}, rowColumns={}, headerColumns={}",
                    record.getRecordNumber(), record.size(), headers.size());
        }

        Map<String, Object> row = new LinkedHashMap<>(Math.max(16, headers.size() * 2));

        for (int i = 0; i < headers.size(); i++) {
            String key = headers.get(i);
            String value = i < record.size() ? record.get(i) : "";
            row.put(key, value);
        }

        return row;
    }

    private void extendHeadersTo(int newSize) {
        int currentSize = headers.size();
        for (int i = currentSize; i < newSize; i++) {
            headers.add("C" + i);
        }
    }

    private static List<String> generateHeaders(int count) {
        List<String> generatedHeaders = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            generatedHeaders.add("C" + i);
        }
        return generatedHeaders;
    }

    private static List<String> normalizeHeaders(List<String> rawHeaders) {
        Map<String, Integer> seen = new HashMap<>();
        List<String> normalized = new ArrayList<>(rawHeaders.size());

        for (int i = 0; i < rawHeaders.size(); i++) {
            String base = rawHeaders.get(i) == null ? "" : rawHeaders.get(i).trim();

            if (base.isEmpty()) {
                base = "C" + i;
            }

            int count = seen.getOrDefault(base, 0) + 1;
            seen.put(base, count);

            normalized.add(count == 1 ? base : base + "_" + count);
        }

        return normalized;
    }

    private static List<String> recordToList(CSVRecord record) {
        List<String> values = new ArrayList<>(record.size());
        for (int i = 0; i < record.size(); i++) {
            values.add(record.get(i));
        }
        return values;
    }

    private String safeRecordToString(CSVRecord record) {
        try {
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < record.size(); i++) {
                if (i > 0) {
                    sb.append(" | ");
                }
                sb.append(record.get(i));
            }

            return sb.toString();
        } catch (Exception e) {
            return "<unable to render record>";
        }
    }

    private String printableDelimiter(char delimiter) {
        if (delimiter == '\t') {
            return "\\t";
        }
        return String.valueOf(delimiter);
    }
}
