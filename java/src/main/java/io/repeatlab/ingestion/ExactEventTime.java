package io.repeatlab.ingestion;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

record ExactEventTime(BigDecimal seconds) {
    private static final BigDecimal DAY = BigDecimal.valueOf(86400);

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
            String value = cell.value();
            BigDecimal fraction = BigDecimal.ZERO;
            int point = value.indexOf('.');
            if (point >= 0) {
                String digits = value.substring(point + 1);
                if (!digits.chars().allMatch(c -> c >= '0' && c <= '9'))
                    throw new IllegalArgumentException("Expected ISO fractional seconds");
                if (!digits.isEmpty()) fraction = new BigDecimal("0." + digits);
                // Keep the dot so the standard parser still validates the fraction's position.
                value = value.substring(0, point + 1);
            }
            LocalDateTime time = value.length() == 10
                    ? LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay()
                    : LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            result = BigDecimal.valueOf(time.toLocalDate().toEpochDay()).multiply(DAY)
                    .add(BigDecimal.valueOf(time.toLocalTime().toSecondOfDay())).add(fraction);
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
