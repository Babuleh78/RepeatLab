package io.repeatlab.ingestion;

/** A real child process for process-boundary failure tests. */
public final class AnalyticsChild {
    private AnalyticsChild() {}
    public static void main(String[] args) throws InterruptedException {
        if (args[0].equals("sleep")) { Thread.sleep(60000); return; }
        for (int index = 0; index < 1000; index++) {
            System.out.print("o".repeat(1024));
            System.err.print("e".repeat(1024));
        }
        System.exit(7);
    }
}
