package io.repeatlab.ingestion;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class WorkerProcess {
    static final int LOG_LIMIT = 2 * 1024 * 1024;
    private static final byte[] TRUNCATED = "\n[worker log truncated]\n".getBytes(StandardCharsets.UTF_8);
    private static final String TIMEOUT = "repeatlab.worker.timeoutSeconds";

    private WorkerProcess() {}

    static int run(List<String> command, Path output) throws IOException, InterruptedException {
        long seconds = Long.parseLong(System.getProperty(TIMEOUT, "3600"));
        if (seconds <= 0) throw new IllegalArgumentException(TIMEOUT + " must be positive");
        Process process = new ProcessBuilder(command).start();
        try {
            // Workers have no stdin protocol. Drain both output streams even after their logs fill.
            process.getOutputStream().close();
            var stdout = drain(process.getInputStream(), output.resolve("worker.stdout.log"));
            var stderr = drain(process.getErrorStream(), output.resolve("worker.stderr.log"));
            if (!process.waitFor(seconds, TimeUnit.SECONDS))
                throw new IOException("Worker exceeded timeout of " + seconds + " seconds");
            awaitLog(stdout);
            awaitLog(stderr);
            return process.exitValue();
        } finally {
            if (process.isAlive()) {
                List<ProcessHandle> children = process.descendants().toList();
                process.destroyForcibly();
                children.forEach(ProcessHandle::destroyForcibly);
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private static FutureTask<Void> drain(InputStream source, Path log) {
        FutureTask<Void> task = new FutureTask<>(() -> {
            try (source; var file = Files.newOutputStream(log, StandardOpenOption.CREATE_NEW)) {
                byte[] buffer = new byte[8192];
                int remaining = LOG_LIMIT;
                boolean truncated = false;
                for (int n; (n = source.read(buffer)) != -1;) {
                    int keep = Math.min(remaining, n);
                    file.write(buffer, 0, keep);
                    remaining -= keep;
                    if (keep < n && !truncated) {
                        file.write(TRUNCATED);
                        truncated = true;
                    }
                }
            }
            return null;
        });
        Thread.startVirtualThread(task);
        return task;
    }

    private static void awaitLog(FutureTask<Void> task) throws InterruptedException, IOException {
        try { task.get(5, TimeUnit.SECONDS); }
        catch (ExecutionException error) { throw new IOException("Could not collect worker log", error.getCause()); }
        catch (TimeoutException error) { throw new IOException("Worker output stream did not close", error); }
    }
}
