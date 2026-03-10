package com.test.dataflowengine.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public final class Utf8ValidationUtils {

    private Utf8ValidationUtils() {
    }

    public static void validateUtf8File(String filePath) throws IOException {
        validateUtf8File(Path.of(filePath));
    }

    public static void validateUtf8File(Path path) throws IOException {
        log.info("[Utf8ValidationUtils] Validating UTF-8 file line by line. path={}", path);

        try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
            ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream(4096);

            long lineNumber = 1L;
            long byteOffset = 0L;
            long lineStartByteOffset = 0L;

            int currentByte;
            while ((currentByte = in.read()) != -1) {
                byteOffset++;

                if (currentByte == '\n') {
                    validateLine(path, lineBuffer.toByteArray(), lineNumber, lineStartByteOffset);
                    lineBuffer.reset();
                    lineNumber++;
                    lineStartByteOffset = byteOffset;
                } else if (currentByte != '\r') {
                    lineBuffer.write(currentByte);
                }
            }

            if (lineBuffer.size() > 0) {
                validateLine(path, lineBuffer.toByteArray(), lineNumber, lineStartByteOffset);
            }
        }

        log.info("[Utf8ValidationUtils] UTF-8 validation passed. path={}", path);
    }

    public static String decodeUtf8Line(byte[] lineBytes, Path path, long lineNumber, long lineStartByteOffset)
            throws IOException {

        CharsetDecoder decoder = StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);

        ByteBuffer input = ByteBuffer.wrap(lineBytes);
        CharBuffer output = CharBuffer.allocate(Math.max(1, lineBytes.length));

        CoderResult result = decoder.decode(input, output, true);
        if (result.isMalformed() || result.isUnmappable()) {
            long badByteOffset = lineStartByteOffset + input.position();

            throw new InvalidUtf8FileException(
                    "Invalid UTF-8 detected. path=" + path +
                    ", line=" + lineNumber +
                    ", byteOffset=" + badByteOffset +
                    ", preview=\"" + buildPreview(lineBytes) + "\"" +
                    ", hexPreview=" + buildHexPreview(lineBytes)
            );
        }

        result = decoder.flush(output);
        if (result.isMalformed() || result.isUnmappable()) {
            throw new InvalidUtf8FileException(
                    "Invalid UTF-8 detected during decoder flush. path=" + path +
                    ", line=" + lineNumber +
                    ", preview=\"" + buildPreview(lineBytes) + "\"" +
                    ", hexPreview=" + buildHexPreview(lineBytes)
            );
        }

        output.flip();
        return output.toString();
    }

    private static void validateLine(Path path, byte[] lineBytes, long lineNumber, long lineStartByteOffset)
            throws IOException {
        decodeUtf8Line(lineBytes, path, lineNumber, lineStartByteOffset);
    }

    private static String buildPreview(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        int max = Math.min(bytes.length, 120);

        for (int i = 0; i < max; i++) {
            int b = bytes[i] & 0xFF;
            if (b >= 32 && b <= 126) {
                sb.append((char) b);
            } else {
                sb.append('?');
            }
        }

        if (bytes.length > max) {
            sb.append("...");
        }

        return sb.toString();
    }

    private static String buildHexPreview(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        int max = Math.min(bytes.length, 32);

        for (int i = 0; i < max; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(String.format("%02X", bytes[i] & 0xFF));
        }

        if (bytes.length > max) {
            sb.append(" ...");
        }

        return sb.toString();
    }
}
