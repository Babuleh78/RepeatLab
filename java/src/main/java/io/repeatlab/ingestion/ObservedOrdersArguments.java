package io.repeatlab.ingestion;

import java.io.IOException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

record ObservedOrdersArguments(Path input, Path clean, Path output, LocalDateTime from, LocalDateTime until) {
    private static final Set<String> OPTIONS =
            Set.of("--input", "--output", "--observed-from", "--observed-until");
    private static final Set<String> CLEAN_BOUND_OPTIONS =
            Set.of("--input", "--clean", "--output", "--observed-from", "--observed-until");
    private static final Pattern BOUND_PATTERN =
            Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?");
    private static final DateTimeFormatter BOUND_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter(Locale.ROOT);

    static ObservedOrdersArguments parse(String[] args) {
        return parse(args, OPTIONS);
    }

    static ObservedOrdersArguments parseWithClean(String[] args) {
        return parse(args, CLEAN_BOUND_OPTIONS);
    }

    private static ObservedOrdersArguments parse(String[] args, Set<String> options) {
        if (args.length != 1 + 2 * options.size()) return null;
        Map<String, String> parameters = new HashMap<>();
        for (int i = 1; i < args.length; i += 2) {
            if (!options.contains(args[i]) || args[i + 1].isBlank()
                    || parameters.putIfAbsent(args[i], args[i + 1]) != null) return null;
        }
        try {
            LocalDateTime from = parseBound(parameters.get("--observed-from"));
            LocalDateTime until = parseBound(parameters.get("--observed-until"));
            if (!from.isBefore(until)) return null;
            Path clean = parameters.containsKey("--clean") ? Path.of(parameters.get("--clean")) : null;
            return new ObservedOrdersArguments(
                    Path.of(parameters.get("--input")), clean, Path.of(parameters.get("--output")), from, until);
        } catch (InvalidPathException | DateTimeParseException invalid) {
            return null;
        }
    }

    static LocalDateTime parseBound(String value) {
        if (!BOUND_PATTERN.matcher(value).matches())
            throw new DateTimeParseException("Expected local timestamp with seconds and up to six fractional digits",
                    value, 0);
        LocalDateTime time = LocalDateTime.parse(value);
        if (time.getYear() < 1) throw new DateTimeParseException("Year must be in 0001–9999", value, 0);
        return time;
    }

    ObservedOrdersArguments resolvePaths() throws IOException {
        Path target = resolveOutput(output);
        Path source = resolveSource(input, target, "orders");
        Path cleanSource = clean == null ? null : resolveSource(clean, target, "clean");
        return new ObservedOrdersArguments(source, cleanSource, target, from, until);
    }

    static Path resolveOutput(Path output) throws IOException {
        Path target = output.toAbsolutePath();
        Path parent = target.getParent();
        return parent == null ? target : parent.toRealPath().resolve(target.getFileName()).normalize();
    }

    static Path resolveSource(Path source, Path target, String name) throws IOException {
        boolean exists = Files.isDirectory(source);
        Path location = exists ? source.toRealPath() : prospectivePath(source);
        if (target.startsWith(location) || location.startsWith(target))
            throw new DiagnosticFailure("Output directory overlaps the input " + name + " directory");
        // Let normal validation diagnose a missing input at its original path.
        return exists ? location : source;
    }

    private static Path prospectivePath(Path source) throws IOException {
        Path path = source.toAbsolutePath();
        Set<Path> followed = new HashSet<>();
        while (true) {
            Path prefix = path;
            while (prefix != null && !Files.exists(prefix, LinkOption.NOFOLLOW_LINKS))
                prefix = prefix.getParent();
            if (prefix == null) return path.normalize();
            Path suffix = prefix.relativize(path);
            if (!Files.isSymbolicLink(prefix) || Files.exists(prefix))
                return prefix.toRealPath().resolve(suffix).normalize();

            // A dangling link in the input could become live when we reserve the output.
            Path parent = prefix.getParent().toRealPath();
            Path link = parent.resolve(prefix.getFileName());
            if (!followed.add(link)) throw new FileSystemLoopException(link.toString());
            path = parent.resolve(Files.readSymbolicLink(link)).resolve(suffix);
        }
    }

    void requireCoverage(List<OrdersDataset.Order> orders) throws IOException {
        for (OrdersDataset.Order order : orders) {
            if (order.time().isBefore(from) || !order.time().isBefore(until))
                throw new DiagnosticFailure("Order timestamp is outside the declared observation interval");
        }
    }

    Map<String, Object> observation() {
        return Json.object("observed_from", BOUND_FORMAT.format(from),
                "observed_until", BOUND_FORMAT.format(until));
    }
}
