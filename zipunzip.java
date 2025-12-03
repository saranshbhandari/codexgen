package com.test.dataflowengine.processors.taskprocessors;

import com.test.dataflowengine.models.ETLTask;
import com.test.dataflowengine.models.JobDetails;
import com.test.dataflowengine.models.TaskStatus;
import com.test.dataflowengine.models.tasksettings.ArchiveType;
import com.test.dataflowengine.models.tasksettings.ZipUnzipMode;
import com.test.dataflowengine.models.tasksettings.ZipUnzipTaskSettings;
import com.test.dataflowengine.variablemanager.variablestore.VariableStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

@Service
@Slf4j
public class ZipUnzipTaskProcessor implements ITaskProcessor {

    @Override
    public TaskStatus ProcessTask(ETLTask task, JobDetails jd, VariableStore varStore) {
        String logPrefix = MessageFormat.format(
                "ZipUnzip Task. TaskId:{0}, TName:{1}, JobId:{2} - ",
                task.getTaskId(), task.getTaskName(), jd.getJobId());

        try {
            ZipUnzipTaskSettings settings = task.getSettingsAsType(ZipUnzipTaskSettings.class);
            if (settings == null) {
                log.error("{}Settings are null.", logPrefix);
                return TaskStatus.Failure;
            }

            log.info("{}Starting task. Mode={}", logPrefix, settings.getMode());

            if (settings.getMode() == ZipUnzipMode.zip) {
                return processZipMode(settings, logPrefix);
            } else if (settings.getMode() == ZipUnzipMode.unzip) {
                return processUnzipMode(settings, logPrefix);
            } else {
                log.error("{}Unsupported mode: {}", logPrefix, settings.getMode());
                return TaskStatus.Failure;
            }
        } catch (Exception ex) {
            log.error("{}Task Failed. Error: {}", logPrefix, ex.getMessage(), ex);
            return TaskStatus.Failure;
        }
    }

    // -------------------------------------------------------------------------
    //  MODE = ZIP
    // -------------------------------------------------------------------------

    private TaskStatus processZipMode(ZipUnzipTaskSettings settings, String logPrefix) throws IOException {
        ArchiveType type = settings.getZipArchiveType();
        if (type == null) {
            log.error("{}ArchiveType is null for zip mode.", logPrefix);
            return TaskStatus.Failure;
        }

        switch (type) {
            case zip:
                return zipAsZip(settings, logPrefix);
            case tar:
                return zipAsTar(settings, logPrefix);
            case gz:
                return zipAsGz(settings, logPrefix);
            case _7z:
                return zipAs7z(settings, logPrefix);
            case rar: // explicitly skipped
            default:
                log.error("{}ArchiveType {} not supported for zip mode.", logPrefix, type);
                return TaskStatus.Failure;
        }
    }

    // -------- ZIP (zip file) --------

    private TaskStatus zipAsZip(ZipUnzipTaskSettings settings, String logPrefix) throws IOException {
        Path sourcePath = Paths.get(settings.getZipSourcePath());
        Path destFolder = Paths.get(settings.getZipDestinationFolder());
        Files.createDirectories(destFolder);

        String fileName = ensureExtension(settings.getZipFileName(), ".zip");
        Path zipFile = destFolder.resolve(fileName);

        log.info("{}Creating ZIP {} from source {}", logPrefix, zipFile, sourcePath);

        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
            if (Files.isDirectory(sourcePath)) {
                zipDirectoryToZip(sourcePath, zos, logPrefix);
            } else if (Files.isRegularFile(sourcePath)) {
                zipSingleFileToZip(sourcePath, zos, logPrefix);
            } else {
                log.error("{}Source path does not exist: {}", logPrefix, sourcePath);
                return TaskStatus.Failure;
            }
        }

        log.info("{}ZIP created at {}", logPrefix, zipFile);
        return TaskStatus.Success;
    }

    private void zipDirectoryToZip(Path rootDir, ZipOutputStream zos, String logPrefix) throws IOException {
        Files.walkFileTree(rootDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (!dir.equals(rootDir)) {
                    Path relative = rootDir.relativize(dir);
                    String entryName = relative.toString().replace("\\", "/") + "/";
                    zos.putNextEntry(new ZipEntry(entryName));
                    zos.closeEntry();
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path relative = rootDir.relativize(file);
                String entryName = relative.toString().replace("\\", "/");
                log.debug("{}Adding file to ZIP: {}", logPrefix, entryName);
                zos.putNextEntry(new ZipEntry(entryName));
                Files.copy(file, zos);
                zos.closeEntry();
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void zipSingleFileToZip(Path file, ZipOutputStream zos, String logPrefix) throws IOException {
        String entryName = file.getFileName().toString();
        log.debug("{}Adding single file to ZIP: {}", logPrefix, entryName);
        zos.putNextEntry(new ZipEntry(entryName));
        Files.copy(file, zos);
        zos.closeEntry();
    }

    // -------- TAR (.tar) --------

    private TaskStatus zipAsTar(ZipUnzipTaskSettings settings, String logPrefix) throws IOException {
        Path sourcePath = Paths.get(settings.getZipSourcePath());
        Path destFolder = Paths.get(settings.getZipDestinationFolder());
        Files.createDirectories(destFolder);

        String fileName = ensureExtension(settings.getZipFileName(), ".tar");
        Path tarFile = destFolder.resolve(fileName);

        log.info("{}Creating TAR {} from source {}", logPrefix, tarFile, sourcePath);

        try (OutputStream fos = Files.newOutputStream(tarFile);
             TarArchiveOutputStream taos = new TarArchiveOutputStream(fos)) {

            taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

            if (Files.isDirectory(sourcePath)) {
                tarDirectory(sourcePath, taos, logPrefix);
            } else if (Files.isRegularFile(sourcePath)) {
                tarSingleFile(sourcePath, taos, logPrefix);
            } else {
                log.error("{}Source path does not exist: {}", logPrefix, sourcePath);
                return TaskStatus.Failure;
            }
        }

        log.info("{}TAR created at {}", logPrefix, tarFile);
        return TaskStatus.Success;
    }

    private void tarDirectory(Path rootDir, TarArchiveOutputStream taos, String logPrefix) throws IOException {
        Files.walkFileTree(rootDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path relative = rootDir.relativize(file);
                String entryName = relative.toString().replace("\\", "/");
                log.debug("{}Adding file to TAR: {}", logPrefix, entryName);

                TarArchiveEntry entry = new TarArchiveEntry(file.toFile(), entryName);
                entry.setSize(Files.size(file));
                taos.putArchiveEntry(entry);
                Files.copy(file, taos);
                taos.closeArchiveEntry();

                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void tarSingleFile(Path file, TarArchiveOutputStream taos, String logPrefix) throws IOException {
        String entryName = file.getFileName().toString();
        log.debug("{}Adding single file to TAR: {}", logPrefix, entryName);

        TarArchiveEntry entry = new TarArchiveEntry(file.toFile(), entryName);
        entry.setSize(Files.size(file));
        taos.putArchiveEntry(entry);
        Files.copy(file, taos);
        taos.closeArchiveEntry();
    }

    // -------- GZIP (.gz) – single file --------

    private TaskStatus zipAsGz(ZipUnzipTaskSettings settings, String logPrefix) throws IOException {
        Path sourcePath = Paths.get(settings.getZipSourcePath());
        if (!Files.isRegularFile(sourcePath)) {
            log.error("{}GZIP expects a single file as source, but got: {}", logPrefix, sourcePath);
            return TaskStatus.Failure;
        }

        Path destFolder = Paths.get(settings.getZipDestinationFolder());
        Files.createDirectories(destFolder);

        String fileName = ensureExtension(settings.getZipFileName(), ".gz");
        Path gzFile = destFolder.resolve(fileName);

        log.info("{}Creating GZ {} from {}", logPrefix, gzFile, sourcePath);

        try (InputStream in = Files.newInputStream(sourcePath);
             OutputStream out = Files.newOutputStream(gzFile);
             GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(out)) {

            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) != -1) {
                gzos.write(buffer, 0, len);
            }
        }

        log.info("{}GZ created at {}", logPrefix, gzFile);
        return TaskStatus.Success;
    }

    // -------- 7Z (.7z) --------

    private TaskStatus zipAs7z(ZipUnzipTaskSettings settings, String logPrefix) throws IOException {
        Path sourcePath = Paths.get(settings.getZipSourcePath());
        Path destFolder = Paths.get(settings.getZipDestinationFolder());
        Files.createDirectories(destFolder);

        String fileName = ensureExtension(settings.getZipFileName(), ".7z");
        Path sevenZFile = destFolder.resolve(fileName);

        log.info("{}Creating 7z {} from source {}", logPrefix, sevenZFile, sourcePath);

        try (SevenZOutputFile sevenZ = new SevenZOutputFile(sevenZFile.toFile())) {
            if (Files.isDirectory(sourcePath)) {
                addDirectoryTo7z(sevenZ, sourcePath, sourcePath, logPrefix);
            } else if (Files.isRegularFile(sourcePath)) {
                addFileTo7z(sevenZ, sourcePath, sourcePath.getFileName().toString(), logPrefix);
            } else {
                log.error("{}Source path does not exist: {}", logPrefix, sourcePath);
                return TaskStatus.Failure;
            }
        }

        log.info("{}7z created at {}", logPrefix, sevenZFile);
        return TaskStatus.Success;
    }

    private void addDirectoryTo7z(SevenZOutputFile sevenZ, Path root, Path dir, String logPrefix) throws IOException {
        Files.list(dir).forEach(path -> {
            try {
                if (Files.isDirectory(path)) {
                    addDirectoryTo7z(sevenZ, root, path, logPrefix);
                } else {
                    String relativeName = root.relativize(path).toString().replace("\\", "/");
                    addFileTo7z(sevenZ, path, relativeName, logPrefix);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void addFileTo7z(SevenZOutputFile sevenZ, Path file, String entryName, String logPrefix) throws IOException {
        log.debug("{}Adding file to 7z: {}", logPrefix, entryName);
        SevenZArchiveEntry entry = sevenZ.createArchiveEntry(file.toFile(), entryName);
        sevenZ.putArchiveEntry(entry);
        try (InputStream in = Files.newInputStream(file)) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) != -1) {
                sevenZ.write(buffer, 0, len);
            }
        }
        sevenZ.closeArchiveEntry();
    }

    // -------------------------------------------------------------------------
    //  MODE = UNZIP
    // -------------------------------------------------------------------------

    private TaskStatus processUnzipMode(ZipUnzipTaskSettings settings, String logPrefix) throws IOException {
        Path archivePath = Paths.get(settings.getUnzipFilePath());
        if (!Files.exists(archivePath)) {
            log.error("{}Archive to unzip does not exist: {}", logPrefix, archivePath);
            return TaskStatus.Failure;
        }

        Path destFolder = Paths.get(settings.getUnzipDestinationFolder());
        Files.createDirectories(destFolder);

        ArchiveType type = inferArchiveTypeFromExtension(archivePath);
        log.info("{}Inferred archive type {} for {}", logPrefix, type, archivePath);

        switch (type) {
            case zip:
                return unzipZip(archivePath, destFolder, logPrefix);
            case tar:
                return untar(archivePath, destFolder, logPrefix);
            case gz:
                return ungzip(archivePath, destFolder, logPrefix);
            case _7z:
                return un7z(archivePath, destFolder, logPrefix);
            case rar:
            default:
                log.error("{}ArchiveType {} not supported for unzip mode.", logPrefix, type);
                return TaskStatus.Failure;
        }
    }

    // -------- unzip ZIP --------

    private TaskStatus unzipZip(Path zipFile, Path destFolder, String logPrefix) throws IOException {
        log.info("{}Unzipping ZIP {} to {}", logPrefix, zipFile, destFolder);

        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                Path newPath = resolveEntryPath(destFolder, entry.getName());
                if (newPath == null) {
                    log.error("{}Blocked malicious ZIP entry: {}", logPrefix, entry.getName());
                    zis.closeEntry();
                    continue;
                }

                if (entry.isDirectory()) {
                    Files.createDirectories(newPath);
                } else {
                    Files.createDirectories(newPath.getParent());
                    try (OutputStream os = Files.newOutputStream(newPath)) {
                        zis.transferTo(os);
                    }
                }
                zis.closeEntry();
            }
        }

        log.info("{}ZIP unzip complete.", logPrefix);
        return TaskStatus.Success;
    }

    // -------- untar TAR --------

    private TaskStatus untar(Path tarFile, Path destFolder, String logPrefix) throws IOException {
        log.info("{}Untarring {} to {}", logPrefix, tarFile, destFolder);

        try (InputStream fis = Files.newInputStream(tarFile);
             TarArchiveInputStream tais = new TarArchiveInputStream(fis)) {

            TarArchiveEntry entry;
            byte[] buffer = new byte[8192];

            while ((entry = tais.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    Path dirPath = resolveEntryPath(destFolder, entry.getName());
                    if (dirPath == null) {
                        log.error("{}Blocked malicious TAR entry: {}", logPrefix, entry.getName());
                        continue;
                    }
                    Files.createDirectories(dirPath);
                } else {
                    Path filePath = resolveEntryPath(destFolder, entry.getName());
                    if (filePath == null) {
                        log.error("{}Blocked malicious TAR entry: {}", logPrefix, entry.getName());
                        continue;
                    }
                    Files.createDirectories(filePath.getParent());
                    try (OutputStream out = Files.newOutputStream(filePath)) {
                        int len;
                        while ((len = tais.read(buffer)) != -1) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
            }
        }

        log.info("{}TAR extraction complete.", logPrefix);
        return TaskStatus.Success;
    }

    // -------- ungzip GZ (single file) --------

    private TaskStatus ungzip(Path gzFile, Path destFolder, String logPrefix) throws IOException {
        log.info("{}Un-gzipping {} to {}", logPrefix, gzFile, destFolder);

        String outName = stripExtension(gzFile.getFileName().toString(), ".gz");
        Path outputFile = destFolder.resolve(outName);

        try (InputStream fis = Files.newInputStream(gzFile);
             GzipCompressorInputStream gzis = new GzipCompressorInputStream(fis);
             OutputStream out = Files.newOutputStream(outputFile)) {

            byte[] buffer = new byte[8192];
            int len;
            while ((len = gzis.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
        }

        log.info("{}GZ extraction complete. Output={}", logPrefix, outputFile);
        return TaskStatus.Success;
    }

    // -------- un7z 7Z --------

    private TaskStatus un7z(Path sevenZPath, Path destFolder, String logPrefix) throws IOException {
        log.info("{}Extracting 7z {} to {}", logPrefix, sevenZPath, destFolder);

        try (SevenZFile sevenZFile = new SevenZFile(sevenZPath.toFile())) {
            SevenZArchiveEntry entry;
            byte[] buffer = new byte[8192];

            while ((entry = sevenZFile.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    Path dirPath = resolveEntryPath(destFolder, entry.getName());
                    if (dirPath == null) {
                        log.error("{}Blocked malicious 7z entry: {}", logPrefix, entry.getName());
                        continue;
                    }
                    Files.createDirectories(dirPath);
                } else {
                    Path filePath = resolveEntryPath(destFolder, entry.getName());
                    if (filePath == null) {
                        log.error("{}Blocked malicious 7z entry: {}", logPrefix, entry.getName());
                        continue;
                    }
                    Files.createDirectories(filePath.getParent());
                    try (OutputStream out = Files.newOutputStream(filePath)) {
                        int len;
                        while ((len = sevenZFile.read(buffer)) != -1) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
            }
        }

        log.info("{}7z extraction complete.", logPrefix);
        return TaskStatus.Success;
    }

    // -------------------------------------------------------------------------
    //  HELPERS
    // -------------------------------------------------------------------------

    private ArchiveType inferArchiveTypeFromExtension(Path path) {
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);

        if (name.endsWith(".zip")) return ArchiveType.zip;
        if (name.endsWith(".tar")) return ArchiveType.tar;
        if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) {
            // if you later want to treat .tar.gz specially, you can change this
            return ArchiveType.gz;
        }
        if (name.endsWith(".gz")) return ArchiveType.gz;
        if (name.endsWith(".7z")) return ArchiveType._7z;
        if (name.endsWith(".rar")) return ArchiveType.rar;

        // default – treat as zip to avoid nulls
        return ArchiveType.zip;
    }

    private String ensureExtension(String baseName, String ext) {
        String lower = baseName.toLowerCase(Locale.ROOT);
        if (lower.endsWith(ext)) {
            return baseName;
        }
        return baseName + ext;
    }

    private String stripExtension(String fileName, String ext) {
        String lower = fileName.toLowerCase(Locale.ROOT);
        if (lower.endsWith(ext)) {
            return fileName.substring(0, fileName.length() - ext.length());
        }
        return fileName;
    }

    /**
     * Prevent Zip Slip / Tar Slip etc.
     */
    private Path resolveEntryPath(Path destDir, String entryName) {
        Path normalized = destDir.resolve(entryName).normalize();
        if (!normalized.startsWith(destDir)) {
            return null;
        }
        return normalized;
    }
}
