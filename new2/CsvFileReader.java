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
import java.util.*;

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
        log.info("[CSVFileReader] Opening CSV parser. path={}", fs.getFilePath());

        validateSettings();

        try {
            Utf8ValidationUtils.validateUtf8File(fs.getFilePath());
        } catch (InvalidUtf8FileException e) {
            log.error("[CSVFileReader] Invalid UTF-8 file. path={}, error={}", fs.getFilePath(), e.getMessage());
            throw e;
        }

        char delim = resolveDelimiterChar(fs);

        this.reader = Files.newBufferedReader(Path.of(fs.getFilePath()), StandardCharsets.UTF_8);

        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delim)
                .setQuote('"')
                .setEscape(null)
                .setIgnoreEmptyLines(true)
                .setTrim(false)
                .build();

        this.parser = new CSVParser(reader, format);
        this.iterator = parser.iterator();

        bufferedFirstRow = null;
        headers.clear();

        if (!iterator.hasNext()) {
            log.info("[CSVFileReader] CSV file is empty. path={}", fs.getFilePath());
            return;
        }

        CSVRecord first;
        try {
            first = iterator.next();
        } catch (Exception e) {
            log.error("[CSVFileReader] Failed to read first CSV record. path={}", fs.getFilePath(), e);
            throw e;
        }

        if (Boolean.TRUE.equals(fs.isFirstRowColumn())) {
            headers = normalizeHeaders(recordToList(first));
            log.debug("[CSVFileReader] Header row detected. columns={}, headers={}", headers.size(), headers);
        } else {
            headers = generateHeaders(first.size());
            bufferedFirstRow = rowFromRecord(first);
            log.debug("[CSVFileReader] No header row. Generated columns={}, headers={}", headers.size(), headers);
        }

        log.info("[CSVFileReader] CSV reader opened successfully. path={}, delimiter={}, isFirstRowColumn={}, columns={}",
                fs.getFilePath(), fs.getFileDelimiter(), fs.isFirstRowColumn(), headers.size());
    }

    @Override
    public List<Map<String, Object>> readBatch(int batchSize) throws Exception {
        log.info("[CSVFileReader] Reading batch. batchSize={}", batchSize);

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
            CSVRecord rec = null;

            try {
                rec = iterator.next();
                batch.add(rowFromRecord(rec));
            } catch (Exception e) {
                if (rec != null) {
                    log.error("[CSVFileReader] Error parsing CSV record. path={}, recordNumber={}, row={}",
                            fs.getFilePath(), rec.getRecordNumber(), safeRecordToString(rec), e);
                } else {
                    log.error("[CSVFileReader] Error reading CSV record before record materialization. path={}",
                            fs.getFilePath(), e);
                }
                throw e;
            }
        }

        if (batch.isEmpty()) {
            return null;
        }

        log.debug("[CSVFileReader] Batch read complete. path={}, rowsRead={}", fs.getFilePath(), batch.size());
        return batch;
    }

    @Override
    public void close() {
        log.info("[CSVFileReader] Closing CSV parser and reader. path={}", fs.getFilePath());

        if (parser != null) {
            try {
                parser.close();
            } catch (Exception e) {
                log.error("[CSVFileReader] Error closing parser", e);
            }
            parser = null;
        }

        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                log.error("[CSVFileReader] Error closing reader", e);
            }
            reader = null;
        }

        iterator = null;
    }

    private void validateSettings() {
        log.info("[CSVFileReader] Validating file settings. path={}", fs != null ? fs.getFilePath() : null);

        if (fs == null) {
            throw new IllegalArgumentException("FileSettings is null");
        }

        if (fs.getFilePath() == null || fs.getFilePath().trim().isEmpty()) {
            log.error("[CSVFileReader] Error. filePath is empty or null");
            throw new IllegalArgumentException("filePath is required");
        }

        if (fs.getFileType() == null || !"csv".equalsIgnoreCase(fs.getFileType().trim())) {
            log.error("[CSVFileReader] Error. fileType must be 'csv'");
            throw new IllegalArgumentException("fileType must be 'csv'");
        }

        if (fs.getFileDelimiter() == null) {
            log.error("[CSVFileReader] Error. fileDelimiter is null");
            throw new IllegalArgumentException("fileDelimiter is required");
        }

        if (fs.getFileDelimiter() == FileDelimiter.CUSTOM) {
            if (fs.getCustomDelimiter() == null || fs.getCustomDelimiter().isEmpty()) {
                log.error("[CSVFileReader] Error. customDelimiter is null or empty when fileDelimiter=CUSTOM");
                throw new IllegalArgumentException("customDelimiter is required when fileDelimiter=CUSTOM");
            }
        }
    }

    private static char resolveDelimiterChar(FileSettings fs) {
        log.info("[CSVFileReader] Resolving delimiter char. fileDelimiter={}", fs.getFileDelimiter());

        FileDelimiter d = fs.getFileDelimiter();

        switch (d) {
            case COMMA:
                return ',';

            case TAB:
                return '\t';

            case PIPE:
                return '|';

            case CUSTOM:
                String cd = fs.getCustomDelimiter();
                if (cd.length() != 1) {
                    log.warn("[CSVFileReader] customDelimiter has length {}. Apache Commons CSV supports only 1 char delimiter. Using first char '{}'",
                            cd.length(), cd.charAt(0));
                }
                return cd.charAt(0);

            default:
                return ',';
        }
    }

    private Map<String, Object> rowFromRecord(CSVRecord rec) {
        if (rec.size() > headers.size()) {
            extendHeadersTo(rec.size());
        }

        Map<String, Object> row = new LinkedHashMap<>(Math.max(headers.size(), 16));

        for (int i = 0; i < headers.size(); i++) {
            String key = headers.get(i);
            String val = (i < rec.size()) ? rec.get(i) : "";
            row.put(key, val);
        }

        return row;
    }

    private void extendHeadersTo(int newSize) {
        int old = headers.size();

        for (int i = old; i < newSize; i++) {
            headers.add("C" + i);
        }

        log.warn("[CSVFileReader] Extended headers due to wider row. oldSize={}, newSize={}, headers={}",
                old, newSize, headers);
    }

    private static List<String> generateHeaders(int count) {
        List<String> h = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            h.add("C" + i);
        }

        return h;
    }

    private static List<String> normalizeHeaders(List<String> raw) {
        Map<String, Integer> seen = new HashMap<>();
        List<String> out = new ArrayList<>(raw.size());

        for (int i = 0; i < raw.size(); i++) {
            String base = raw.get(i) == null ? "" : raw.get(i).trim();

            if (base.isEmpty()) {
                base = "C" + i;
            }

            int n = seen.getOrDefault(base, 0) + 1;
            seen.put(base, n);

            out.add(n == 1 ? base : base + "_" + n);
        }

        return out;
    }

    private static List<String> recordToList(CSVRecord rec) {
        List<String> out = new ArrayList<>(rec.size());

        for (int i = 0; i < rec.size(); i++) {
            out.add(rec.get(i));
        }

        return out;
    }

    private String safeRecordToString(CSVRecord rec) {
        try {
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < rec.size(); i++) {
                if (i > 0) {
                    sb.append(" | ");
                }

                String value = rec.get(i);
                sb.append(value == null ? "" : value);
            }

            return sb.toString();
        } catch (Exception e) {
            return "<unable to render record>";
        }
    }
}
