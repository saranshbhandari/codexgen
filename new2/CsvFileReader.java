package com.test.dataflowengine.processors.taskprocessors.datataskprocessorhelpers.datareaders;

import com.test.dataflowengine.models.enums.FileDelimiter;
import com.test.dataflowengine.models.tasksettings.DataTaskSettings;
import com.test.dataflowengine.models.tasksettings.subsettings.FileSettings;
import com.test.dataflowengine.processors.taskprocessors.datataskprocessorhelpers.Contracts.DataReader;
import com.test.dataflowengine.utils.Utf8FileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Slf4j
public class CsvFileReader implements DataReader {

    private enum ReaderMode {
        NORMAL_UTF8,
        FALLBACK_RECOVERY
    }

    private final FileSettings fs;

    private ReaderMode mode;
    private CSVFormat csvFormat;

    // Normal mode
    private BufferedReader reader;
    private CSVParser parser;
    private Iterator<CSVRecord> iterator;

    // Fallback mode
    private InputStream rawInputStream;
    private long fallbackLineNumber = 0L;

    private List<String> headers = new ArrayList<>();
    private Map<String, Object> bufferedFirstRow;

    public CsvFileReader(DataTaskSettings settings) {
        this.fs = settings.getSource().getFileSettings();
    }

    @Override
    public void open() throws Exception {
        log.info("[CsvFileReader] Opening CSV reader. path={}", fs.getFilePath());

        validateSettings();

        char delimiter = resolveDelimiterChar(fs);

        this.csvFormat = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter)
                .setQuote('"')
                .setIgnoreEmptyLines(true)
                .setTrim(false)
                .get();

        boolean validUtf8 = Utf8FileUtils.isValidUtf8File(fs.getFilePath());

        if (validUtf8) {
            openNormalMode();
        } else {
            openFallbackMode();
        }

        log.info("[CsvFileReader] Reader opened. path={}, mode={}, delimiter='{}', headerCount={}",
                fs.getFilePath(), mode, printableDelimiter(delimiter), headers.size());
    }

    private void openNormalMode() throws Exception {
        this.mode = ReaderMode.NORMAL_UTF8;
        log.info("[CsvFileReader] File is valid UTF-8. Using normal CSV parsing. path={}", fs.getFilePath());

        this.reader = Files.newBufferedReader(Path.of(fs.getFilePath()), StandardCharsets.UTF_8);
        this.parser = csvFormat.parse(reader);
        this.iterator = parser.iterator();

        headers.clear();
        bufferedFirstRow = null;

        if (!iterator.hasNext()) {
            log.warn("[CsvFileReader] CSV file is empty. path={}", fs.getFilePath());
            return;
        }

        CSVRecord firstRecord = iterator.next();

        if (Boolean.TRUE.equals(fs.isFirstRowColumn())) {
            headers = normalizeHeaders(recordToList(firstRecord));
            log.info("[CsvFileReader] Header row detected in normal mode. headers={}", headers);
        } else {
            headers = generateHeaders(firstRecord.size());
            bufferedFirstRow = rowFromRecord(firstRecord);
            log.info("[CsvFileReader] No header row in normal mode. Generated headers={}", headers);
        }
    }

    private void openFallbackMode() throws Exception {
        this.mode = ReaderMode.FALLBACK_RECOVERY;
        log.warn("[CsvFileReader] File is not valid UTF-8. Switching to fallback recovery mode. path={}",
                fs.getFilePath());

        this.rawInputStream = new BufferedInputStream(Files.newInputStream(Path.of(fs.getFilePath())));
        this.fallbackLineNumber = 0L;

        headers.clear();
        bufferedFirstRow = null;

        initializeFallbackHeaders();
    }

    @Override
    public List<Map<String, Object>> readBatch(int batchSize) throws Exception {
        if (batchSize <= 0) {
            batchSize = 1000;
        }

        log.debug("[CsvFileReader] Reading batch. path={}, mode={}, batchSize={}",
                fs.getFilePath(), mode, batchSize);

        if (mode == ReaderMode.NORMAL_UTF8) {
            return readBatchNormal(batchSize);
        } else {
            return readBatchFallback(batchSize);
        }
    }

    private List<Map<String, Object>> readBatchNormal(int batchSize) throws Exception {
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
                    log.error("[CsvFileReader] Error parsing CSV row in normal mode. path={}, recordNumber={}, row={}",
                            fs.getFilePath(), record.getRecordNumber(), safeRecordToString(record), e);
                } else {
                    log.error("[CsvFileReader] Error parsing CSV row in normal mode before record creation. path={}",
                            fs.getFilePath(), e);
                }
                throw e;
            }
        }

        return batch.isEmpty() ? null : batch;
    }

    private List<Map<String, Object>> readBatchFallback(int batchSize) throws Exception {
        if (rawInputStream == null) {
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

        while (batch.size() < batchSize) {
            RawLine rawLine = readNextRawLine();
            if (rawLine == null) {
                break;
            }

            if (rawLine.bytes.length == 0) {
                continue;
            }

            String lineText = Utf8FileUtils.decodeLineBestEffort(rawLine.bytes);

            if (lineText.trim().isEmpty()) {
                continue;
            }

            try {
                List<String> values = parseSingleLine(lineText, rawLine.lineNumber);

                if (values == null || values.isEmpty()) {
                    continue;
                }

                batch.add(valuesToRow(values, rawLine.lineNumber));

            } catch (Exception e) {
                log.error("[CsvFileReader] Error parsing CSV row in fallback mode. path={}, lineNumber={}, rawLine={}",
                        fs.getFilePath(), rawLine.lineNumber, safeText(lineText), e);
            }
        }

        return batch.isEmpty() ? null : batch;
    }

    private void initializeFallbackHeaders() throws Exception {
        while (true) {
            RawLine rawLine = readNextRawLine();
            if (rawLine == null) {
                log.warn("[CsvFileReader] CSV file is empty in fallback mode. path={}", fs.getFilePath());
                return;
            }

            String lineText = Utf8FileUtils.decodeLineBestEffort(rawLine.bytes);

            if (lineText.trim().isEmpty()) {
                continue;
            }

            try {
                List<String> values = parseSingleLine(lineText, rawLine.lineNumber);

                if (values == null || values.isEmpty()) {
                    continue;
                }

                if (Boolean.TRUE.equals(fs.isFirstRowColumn())) {
                    headers = normalizeHeaders(values);
                    log.info("[CsvFileReader] Header row detected in fallback mode. lineNumber={}, headers={}",
                            rawLine.lineNumber, headers);
                } else {
                    headers = generateHeaders(values.size());
                    bufferedFirstRow = valuesToRow(values, rawLine.lineNumber);
                    log.info("[CsvFileReader] No header row in fallback mode. Generated headers={}", headers);
                }

                return;

            } catch (Exception e) {
                log.error("[CsvFileReader] Failed parsing initial row in fallback mode. path={}, lineNumber={}, rawLine={}",
                        fs.getFilePath(), rawLine.lineNumber, safeText(lineText), e);
            }
        }
    }

    private List<String> parseSingleLine(String lineText, long lineNumber) throws Exception {
        try (CSVParser lineParser = CSVParser.parse(lineText, csvFormat)) {
            List<CSVRecord> records = lineParser.getRecords();

            if (records.isEmpty()) {
                log.warn("[CsvFileReader] Parsed line produced no records. path={}, lineNumber={}, line={}",
                        fs.getFilePath(), lineNumber, safeText(lineText));
                return Collections.emptyList();
            }

            CSVRecord record = records.get(0);
            List<String> values = new ArrayList<>(record.size());

            for (int i = 0; i < record.size(); i++) {
                values.add(record.get(i));
            }

            if (records.size() > 1) {
                log.warn("[CsvFileReader] Parsed line produced multiple records. path={}, lineNumber={}, recordCount={}",
                        fs.getFilePath(), lineNumber, records.size());
            }

            return values;
        }
    }

    private RawLine readNextRawLine() throws Exception {
        if (rawInputStream == null) {
            return null;
        }

        ByteArrayOutputStream buffer = new ByteArrayOutputStream(4096);
        boolean anyRead = false;

        while (true) {
            int b = rawInputStream.read();

            if (b == -1) {
                if (!anyRead && buffer.size() == 0) {
                    return null;
                }

                fallbackLineNumber++;
                return new RawLine(fallbackLineNumber, buffer.toByteArray());
            }

            anyRead = true;

            if (b == '\n') {
                fallbackLineNumber++;
                return new RawLine(fallbackLineNumber, buffer.toByteArray());
            }

            if (b != '\r') {
                buffer.write(b);
            }
        }
    }

    private Map<String, Object> rowFromRecord(CSVRecord record) {
        if (record.size() > headers.size()) {
            int oldSize = headers.size();
            extendHeadersTo(record.size());
            log.warn("[CsvFileReader] Row wider than header in normal mode. recordNumber={}, oldHeaderCount={}, newHeaderCount={}",
                    record.getRecordNumber(), oldSize, headers.size());
        }

        Map<String, Object> row = new LinkedHashMap<>(Math.max(16, headers.size() * 2));

        for (int i = 0; i < headers.size(); i++) {
            String key = headers.get(i);
            String value = i < record.size() ? record.get(i) : "";
            row.put(key, value);
        }

        return row;
    }

    private Map<String, Object> valuesToRow(List<String> values, long lineNumber) {
        if (values.size() > headers.size()) {
            int oldSize = headers.size();
            extendHeadersTo(values.size());
            log.warn("[CsvFileReader] Row wider than header in fallback mode. lineNumber={}, oldHeaderCount={}, newHeaderCount={}",
                    lineNumber, oldSize, headers.size());
        }

        Map<String, Object> row = new LinkedHashMap<>(Math.max(16, headers.size() * 2));

        for (int i = 0; i < headers.size(); i++) {
            String key = headers.get(i);
            String value = i < values.size() ? values.get(i) : "";
            row.put(key, value);
        }

        return row;
    }

    @Override
    public void close() {
        log.info("[CsvFileReader] Closing CSV reader. path={}, mode={}", fs.getFilePath(), mode);

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

        if (rawInputStream != null) {
            try {
                rawInputStream.close();
            } catch (Exception e) {
                log.error("[CsvFileReader] Error closing raw input stream", e);
            }
            rawInputStream = null;
        }

        iterator = null;
        csvFormat = null;
    }

    private void validateSettings() {
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
            if (fs.getCustomDelimiter().length() != 1) {
                throw new IllegalArgumentException(
                        "Apache Commons CSV supports only single character delimiter. customDelimiter=" + fs.getCustomDelimiter()
                );
            }
        }
    }

    private static char resolveDelimiterChar(FileSettings fs) {
        switch (fs.getFileDelimiter()) {
            case COMMA:
                return ',';
            case TAB:
                return '\t';
            case PIPE:
                return '|';
            case CUSTOM:
                return fs.getCustomDelimiter().charAt(0);
            default:
                return ',';
        }
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
        return delimiter == '\t' ? "\\t" : String.valueOf(delimiter);
    }

    private String safeText(String text) {
        if (text == null) {
            return "";
        }
        String normalized = text.replace("\r", "\\r").replace("\n", "\\n");
        return normalized.length() > 500 ? normalized.substring(0, 500) + "..." : normalized;
    }

    private static class RawLine {
        private final long lineNumber;
        private final byte[] bytes;

        private RawLine(long lineNumber, byte[] bytes) {
            this.lineNumber = lineNumber;
            this.bytes = bytes;
        }
    }
}
