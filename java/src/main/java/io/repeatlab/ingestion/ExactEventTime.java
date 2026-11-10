package io.repeatlab.ingestion;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

record ExactEventTime(BigDecimal seconds) {
    private static final BigDecimal DAY = BigDecimal.valueOf(86400);
    private static final Pattern ISO = Pattern.compile("([0-9]{4}-[0-9]{2}-[0-9]{2})(?:T([0-9]{2}):([0-9]{2}):([0-9]{2})(\\.[0-9]+)?)?");

    static ExactEventTime parse(Cell cell, String dateSystem) {
        if (cell.isFormula() || cell.empty() || !cell.scalar()) throw new IllegalArgumentException("Invalid time");
        BigDecimal result;
        if (cell.source_type().equals("n")) {
            BigDecimal serial = new BigDecimal(cell.value());
            if (serial.signum() < 0) throw new IllegalArgumentException("Negative serial");
            long days = serial.setScale(0, RoundingMode.FLOOR).longValueExact();
            boolean date1904 = dateSystem.equals("1904");
            if (!date1904 && days == 60) throw new IllegalArgumentException("Fictitious Excel day");
            LocalDate epoch = date1904 ? LocalDate.of(1904, 1, 1) : LocalDate.of(1899, 12, 31);
            LocalDate date = epoch.plusDays(!date1904 && days > 60 ? days - 1 : days);
            result = BigDecimal.valueOf(date.toEpochDay()).multiply(DAY)
                    .add(serial.subtract(BigDecimal.valueOf(days)).multiply(DAY));
        } else {
            var match = ISO.matcher(cell.value());
            if (!match.matches()) throw new IllegalArgumentException("Expected ISO local time");
            LocalDate date = LocalDate.parse(match.group(1));
            int hour = match.group(2) == null ? 0 : Integer.parseInt(match.group(2));
            int minute = match.group(3) == null ? 0 : Integer.parseInt(match.group(3));
            int second = match.group(4) == null ? 0 : Integer.parseInt(match.group(4));
            date.atTime(hour, minute, second);
            result = BigDecimal.valueOf(date.toEpochDay()).multiply(DAY)
                    .add(BigDecimal.valueOf(hour * 3600L + minute * 60L + second));
            if (match.group(5) != null) result = result.add(new BigDecimal(match.group(5)));
        }
        BigDecimal lower = BigDecimal.valueOf(LocalDate.of(1, 1, 1).toEpochDay()).multiply(DAY);
        BigDecimal upper = BigDecimal.valueOf(LocalDate.of(9999, 12, 31).toEpochDay() + 1).multiply(DAY);
        if (result.compareTo(lower) < 0 || result.compareTo(upper) >= 0)
            throw new IllegalArgumentException("Time outside years 1-9999");
        return new ExactEventTime(result.stripTrailingZeros());
    }

    boolean losesPrecision() { return seconds.compareTo(seconds.setScale(6, RoundingMode.FLOOR)) != 0; }

    String formatted() {
        BigDecimal floored = seconds.setScale(6, RoundingMode.FLOOR);
        long day = floored.divide(DAY, 0, RoundingMode.FLOOR).longValueExact();
        long micros = floored.subtract(BigDecimal.valueOf(day).multiply(DAY)).movePointRight(6).longValueExact();
        LocalDateTime time = LocalDate.ofEpochDay(day).atStartOfDay().plusNanos(micros * 1000);
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(time);
    }
}
