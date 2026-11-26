package io.repeatlab.ingestion;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

record ExactEventTime(LocalDate date, BigDecimal secondsOfDay) implements Comparable<ExactEventTime> {
    private static final BigDecimal DAY = BigDecimal.valueOf(86400);
    private static final BigDecimal HALF = BigDecimal.valueOf(5, 1);
    private static final LocalDate LAST_DATE = LocalDate.of(9999, 12, 31);

    static ExactEventTime of(LocalDateTime time) {
        return new ExactEventTime(time.toLocalDate(), BigDecimal.valueOf(time.toLocalTime().toNanoOfDay(), 9));
    }

    static ExactEventTime parse(Cell cell, String dateSystem) {
        if (cell.isFormula() || cell.empty() || !cell.scalar()) throw new IllegalArgumentException("Invalid time");
        if (cell.source_type().equals("n")) {
            BigDecimal serial = new BigDecimal(cell.value());
            boolean date1904 = dateSystem.equals("1904");
            LocalDate epoch = date1904 ? LocalDate.of(1904, 1, 1) : LocalDate.of(1899, 12, 31);
            long limit = LAST_DATE.toEpochDay() - epoch.toEpochDay() + (date1904 ? 1 : 2);
            if (serial.signum() < 0 || serial.compareTo(BigDecimal.valueOf(limit)) >= 0)
                throw new IllegalArgumentException("Serial outside supported calendar");
            long days = serial.compareTo(BigDecimal.ONE) < 0 ? 0 : serial.longValue();
            if (!date1904 && days == 60) throw new IllegalArgumentException("Fictitious Excel day");
            LocalDate date = epoch.plusDays(!date1904 && days > 60 ? days - 1 : days);
            if (serial.signum() == 0) return new ExactEventTime(date, BigDecimal.ZERO);
            BigDecimal fraction = days == 0 ? serial : serial.subtract(BigDecimal.valueOf(days));
            return new ExactEventTime(date, fraction.multiply(DAY));
        }
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
        if (time.getYear() < 1 || time.getYear() > 9999)
            throw new IllegalArgumentException("Time outside years 1-9999");
        return new ExactEventTime(time.toLocalDate(), BigDecimal.valueOf(time.toLocalTime().toSecondOfDay()).add(fraction));
    }

    @Override public int compareTo(ExactEventTime other) {
        int byDay = date.compareTo(other.date);
        return byDay != 0 ? byDay : secondsOfDay.compareTo(other.secondsOfDay);
    }

    boolean losesPrecision() { return secondsOfDay.stripTrailingZeros().scale() > 6; }

    private long scaled(int places, RoundingMode mode) {
        BigDecimal units = secondsOfDay.scaleByPowerOfTen(places);
        // Tiny scientific-notation serials need no exponent expansion to round to zero.
        if (units.compareTo(HALF) < 0) return 0;
        return units.setScale(0, mode).longValueExact();
    }

    String formatted() {
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(date.atStartOfDay().plusNanos(scaled(6, RoundingMode.DOWN) * 1000));
    }

    LocalDateTime auditTime() {
        long nanos = scaled(9, RoundingMode.HALF_UP);
        if (date.equals(LAST_DATE) && nanos == 86_400_000_000_000L) nanos--;
        return date.atStartOfDay().plusNanos(nanos);
    }
}
