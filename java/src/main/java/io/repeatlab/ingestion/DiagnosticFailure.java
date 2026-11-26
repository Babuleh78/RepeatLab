package io.repeatlab.ingestion;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.sql.SQLException;

/** A reason suitable for operator logs. Messages must contain only application-owned details. */
final class DiagnosticFailure extends IOException {
    private static final long serialVersionUID = 1L;

    DiagnosticFailure(String reason) { super(reason); }

    static String describe(Throwable failure) {
        if (failure instanceof DiagnosticFailure known) return known.getMessage();
        if (failure instanceof WorkbookFailure workbook) {
            if (workbook.getCause() instanceof DiagnosticFailure known) return known.getMessage();
            return "Invalid workbook structure or cell encoding";
        }
        if (failure instanceof NoSuchFileException) return "Required input or directory is missing";
        if (failure instanceof FileAlreadyExistsException) return "Output path already exists";
        if (failure instanceof AccessDeniedException) return "Access to a required file was denied";
        if (failure instanceof com.fasterxml.jackson.core.JsonProcessingException) return "Invalid JSON input";
        if (failure instanceof SQLException) return "SQL operation failed; check numeric bounds and available resources";
        if (failure instanceof InterruptedException) return "Run interrupted";
        if (failure instanceof IOException) return "Could not read or write the required files";
        if (failure instanceof IllegalArgumentException) return "Invalid input or runtime configuration";
        return "The data stage could not be completed";
    }
}
