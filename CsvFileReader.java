package com.test.dataflowengine.processors.taskprocessors.datataskprocessorhelpers.datareaders;

import com.test.dataflowengine.models.enums.FileDelimiter;
import com.test.dataflowengine.models.tasksettings.DataTaskSettings;
import com.test.dataflowengine.models.tasksettings.subsettings.FileSettings;
import com.test.dataflowengine.processors.taskprocessors.datataskprocessorhelpers.Contracts.DataReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
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
        validateSettings();

        char delim = resolveDelimiterChar(fs);

        this.reader = Files.newBufferedReader(Path.of(fs.getFilePath()), StandardCharsets.UTF_8);

        // Commons CSV handles RFC-style quotes, escaped quotes, and multiline fields.
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delim)
                .setQuote('"')
                .setEscape(null) // RFC4180 escaping is "" inside quotes; Commons supports it automatically
                .setIgnoreEmptyLines(true)
                .setTrim(false)  // keep exact spaces; set true if you want trimming
                .build();

        this.parser = new CSVParser(reader, format);
        this.iterator = parser.iterator();

        bufferedFirstRow = null;
        headers.clear();

        if (!iterator.hasNext()) {
            return; // empty file
        }

        CSVRecord first = iterator.next();
        if (Boolean.TRUE.equals(fs.isFirstRowColumn())) {
            headers = normalizeHeaders(recordToList(first));
        } else {
            headers = generateHeaders(first.size());
            bufferedFirstRow = rowFromRecord(first);
        }

        log.debug("CSV reader opened. path={}, delimiter={}, isFirstRowColumn={}, columns={}",
                fs.getFilePath(), fs.getFileDelimiter(), fs.isFirstRowColumn(), headers.size());
    }

    @Override
    public List<Map<String, Object>> readBatch(int batchSize) throws Exception {
        if (batchSize <= 0) batchSize = 1000;
        if (iterator == null) return null;

        List<Map<String, Object>> batch = new ArrayList<>(Math.min(batchSize, 2048));

        if (bufferedFirstRow != null) {
            batch.add(bufferedFirstRow);
            bufferedFirstRow = null;
            if (batch.size() >= batchSize) return batch;
        }

        while (batch.size() < batchSize && iterator.hasNext()) {
            CSVRecord rec = iterator.next();
            batch.add(rowFromRecord(rec));
        }

        return batch.isEmpty() ? null : batch;
    }

    @Override
    public void close() {
        // Close parser first (it wraps reader)
        if (parser != null) {
            try { parser.close(); } catch (Exception ignored) {}
            parser = null;
        }
        if (reader != null) {
            try { reader.close(); } catch (Exception ignored) {}
            reader = null;
        }
        iterator = null;
    }

    // ---------------- helpers ----------------

    private void validateSettings() {
        if (fs == null) throw new IllegalArgumentException("FileSettings is null");
        if (fs.getFilePath() == null || fs.getFilePath().trim().isEmpty())
            throw new IllegalArgumentException("filePath is required");

        if (fs.getFileType() == null || !"csv".equalsIgnoreCase(fs.getFileType().trim()))
            throw new IllegalArgumentException("fileType must be 'csv'");

        if (fs.getFileDelimiter() == null)
            throw new IllegalArgumentException("fileDelimiter is required");

        if (fs.getFileDelimiter() == FileDelimiter.CUSTOM) {
            if (fs.getCustomDelimiter() == null || fs.getCustomDelimiter().isEmpty())
                throw new IllegalArgumentException("customDelimiter is required when fileDelimiter=CUSTOM");
        }
    }

    private static char resolveDelimiterChar(FileSettings fs) {
        FileDelimiter d = fs.getFileDelimiter();
        switch (d) {
            case COMMA: return ',';
            case TAB:   return '\t';
            case PIPE:  return '|';
            case CUSTOM:
                String cd = fs.getCustomDelimiter();
                if (cd.length() != 1) {
                    // Commons CSV delimiter is a char; if user gives multi-char, we can only take the first.
                    log.warn("customDelimiter has length {}. Apache Commons CSV supports only 1 char delimiter. Using first char: '{}'",
                            cd.length(), cd.charAt(0));
                }
                return cd.charAt(0);
            default:
                return ',';
        }
    }

    private Map<String, Object> rowFromRecord(CSVRecord rec) {
        // If a row has more columns than current headers, extend headers to avoid losing data.
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
    }

    private static List<String> generateHeaders(int count) {
        List<String> h = new ArrayList<>(count);
        for (int i = 0; i < count; i++) h.add("C" + i);
        return h;
    }

    private static List<String> normalizeHeaders(List<String> raw) {
        Map<String, Integer> seen = new HashMap<>();
        List<String> out = new ArrayList<>(raw.size());

        for (int i = 0; i < raw.size(); i++) {
            String base = raw.get(i) == null ? "" : raw.get(i).trim();
            if (base.isEmpty()) base = "C" + i;

            int n = seen.getOrDefault(base, 0) + 1;
            seen.put(base, n);

            out.add(n == 1 ? base : base + "_" + n);
        }
        return out;
    }

    private static List<String> recordToList(CSVRecord rec) {
        List<String> out = new ArrayList<>(rec.size());
        for (int i = 0; i < rec.size(); i++) out.add(rec.get(i));
        return out;
    }
}
