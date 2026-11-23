package io.repeatlab.ingestion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

record ObservedOrdersArguments(Path input, Path output, LocalDateTime from, LocalDateTime until) {
    private static final Set<String> OPTIONS =
            Set.of("--input", "--output", "--observed-from", "--observed-until");
    private static final Pattern BOUND_PATTERN =
            Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?");
    private static final DateTimeFormatter BOUND_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter(Locale.ROOT);

    static ObservedOrdersArguments parse(String[] args) {
        if (args.length != 9) return null;
        Map<String, String> parameters = new HashMap<>();
        for (int i = 1; i < args.length; i += 2) {
            if (!OPTIONS.contains(args[i]) || args[i + 1].isBlank()
                    || parameters.putIfAbsent(args[i], args[i + 1]) != null) return null;
        }
        try {
            LocalDateTime from = parseBound(parameters.get("--observed-from"));
            LocalDateTime until = parseBound(parameters.get("--observed-until"));
            if (!from.isBefore(until)) return null;
            return new ObservedOrdersArguments(
                    Path.of(parameters.get("--input")), Path.of(parameters.get("--output")), from, until);
        } catch (InvalidPathException | DateTimeParseException invalid) {
            return null;
        }
    }

    private static LocalDateTime parseBound(String value) {
        if (!BOUND_PATTERN.matcher(value).matches())
            throw new DateTimeParseException("Expected local timestamp with seconds and up to six fractional digits",
                    value, 0);
        LocalDateTime time = LocalDateTime.parse(value);
        if (time.getYear() < 1) throw new DateTimeParseException("Year must be in 0001–9999", value, 0);
        return time;
    }

    ObservedOrdersArguments resolvePaths() throws IOException {
        Path source = input;
        Path target = output.toAbsolutePath();
        Path parent = target.getParent();
        if (parent != null) {
            Path realParent = parent.toRealPath();
            // Invalid input paths still go through orders validation and its diagnostics.
            if (Files.isDirectory(source)) {
                source = source.toRealPath();
                if (realParent.startsWith(source))
                    throw new IOException("Output directory is inside the input orders directory");
            }
            target = realParent.resolve(target.getFileName());
        }
        return new ObservedOrdersArguments(source, target, from, until);
    }

    void requireCoverage(List<OrdersDataset.Order> orders) throws IOException {
        for (OrdersDataset.Order order : orders) {
            if (order.time().isBefore(from) || !order.time().isBefore(until))
                throw new IOException("Order timestamp is outside the declared observation interval");
        }
    }

    Map<String, Object> observation() {
        return Json.object("observed_from", BOUND_FORMAT.format(from),
                "observed_until", BOUND_FORMAT.format(until));
    }
}
