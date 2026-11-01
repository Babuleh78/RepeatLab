package io.repeatlab.ingestion;

import java.io.IOException;

final class WorkbookFailure extends IOException {
    private static final long serialVersionUID = 1L;
    final int sheetIndex;
    final String sheetName;
    final int rowNumber;

    WorkbookFailure(int sheetIndex, String sheetName, int rowNumber, Exception cause) {
        super(cause.getMessage(), cause);
        this.sheetIndex = sheetIndex;
        this.sheetName = sheetName;
        this.rowNumber = rowNumber;
    }
}
