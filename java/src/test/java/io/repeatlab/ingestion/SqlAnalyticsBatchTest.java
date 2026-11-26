package io.repeatlab.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SqlAnalyticsBatchTest {
    @Test void loadsFullBatchAndFinalPartialBatchIntoBothReports(@TempDir Path stage) throws Exception {
        var orders = new ArrayList<OrdersDataset.Order>();
        LocalDateTime time = LocalDateTime.of(2020, 1, 1, 0, 0);
        for (int i = 0; i < 1025; i++) {
            String customer = (i == 1024) ? null : "A";
            orders.add(new OrdersDataset.Order("I" + i, customer, time, "sale", 1,
                    BigInteger.ONE, 1, "0.335"));
        }

        SqlAnalytics.run(orders, stage);
        var monthly = StrictJson.read(stage.resolve("monthly_activity.jsonl"));
        assertEquals(1025, monthly.get("order_count").intValue());
        assertEquals("343.375", monthly.get("total_amount").textValue());
        var missing = StrictJson.read(stage.resolve("missing_customer.jsonl"));
        assertEquals(1, missing.get("missing_customer_order_count").intValue());
        assertEquals("0.335", missing.get("missing_customer_total_amount").textValue());
    }
}
