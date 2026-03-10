package com.test.dataflowengine.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public final class Utf8FileUtils {

    private static final int BUFFER_SIZE = 8192;

    private Utf8FileUtils() {
    }

    public static boolean isValidUtf8File(String filePath) {
        return isValidUtf8File(Path.of(filePath));
    }

    public static boolean isValidUtf8File(Path path) {
        CharsetDecoder decoder = StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);

        byte[] buffer = new byte[BUFFER_SIZE];
        byte[] carry = new byte[4];
        int carryLen = 0;

        try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
            int read;

            while ((read = in.read(buffer)) != -1) {
                byte[] chunk = new byte[carryLen + read];
                System.arraycopy(carry, 0, chunk, 0, carryLen);
                System.arraycopy(buffer, 0, chunk, carryLen, read);

                ByteBuffer byteBuffer = ByteBuffer.wrap(chunk);
                CharBuffer charBuffer = CharBuffer.allocate(chunk.length);

                CoderResult result = decoder.decode(byteBuffer, charBuffer, false);
                if (result.isMalformed() || result.isUnmappable()) {
                    log.warn("[Utf8FileUtils] Invalid UTF-8 detected. path={}, bytePosition={}",
                            path, byteBuffer.position());
                    return false;
                }

                int remaining = byteBuffer.remaining();
                if (remaining > 4) {
                    log.warn("[Utf8FileUtils] Invalid UTF-8 trailing bytes. path={}", path);
                    return false;
                }

                carryLen = remaining;
                for (int i = 0; i < remaining; i++) {
                    carry[i] = byteBuffer.get();
                }
            }

            ByteBuffer finalBuffer = ByteBuffer.wrap(carry, 0, carryLen);
            CharBuffer finalChars = CharBuffer.allocate(8);

            CoderResult finalResult = decoder.decode(finalBuffer, finalChars, true);
            if (finalResult.isMalformed() || finalResult.isUnmappable()) {
                log.warn("[Utf8FileUtils] Invalid UTF-8 at EOF. path={}", path);
                return false;
            }

            finalResult = decoder.flush(finalChars);
            if (finalResult.isMalformed() || finalResult.isUnmappable()) {
                log.warn("[Utf8FileUtils] Invalid UTF-8 during flush. path={}", path);
                return false;
            }

            return true;

        } catch (Exception e) {
            log.error("[Utf8FileUtils] Failed during UTF-8 validation. path={}", path, e);
            return false;
        }
    }

    public static String decodeLineBestEffort(byte[] lineBytes) {
        CharsetDecoder strictDecoder = StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);

        try {
            return strictDecoder.decode(ByteBuffer.wrap(lineBytes)).toString();
        } catch (CharacterCodingException e) {
            log.warn("[Utf8FileUtils] Line is not valid UTF-8. Decoding with replacement.");
            CharsetDecoder replaceDecoder = StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE);

            try {
                return replaceDecoder.decode(ByteBuffer.wrap(lineBytes)).toString();
            } catch (CharacterCodingException ex) {
                return new String(lineBytes, StandardCharsets.ISO_8859_1);
            }
        }
    }
}
