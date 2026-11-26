package io.repeatlab.ingestion;

import java.io.IOException;
import java.math.BigInteger;
import java.util.TreeMap;

/** Normalized monetary coefficient and exponent, independent of SQL and BigDecimal scale limits. */
record ExactDecimal(BigInteger coefficient, BigInteger exponent) {
    static ExactDecimal parse(String text) throws IOException {
        StrictJson.require(OrdersDataset.DECIMAL.matcher(text).matches(), "Invalid exact monetary string");
        String[] parts = text.split("[eE]", 2);
        String mantissa = parts[0];
        int point = mantissa.indexOf('.');
        int fraction = point < 0 ? 0 : mantissa.length() - point - 1;
        String digits = mantissa.replace(".", "").replace("-", "").replace("+", "");
        int end = digits.length();
        while (end > 0 && digits.charAt(end - 1) == '0') end--;
        if (end == 0) return new ExactDecimal(BigInteger.ZERO, BigInteger.ZERO);
        BigInteger value = new BigInteger(digits.substring(0, end));
        BigInteger power = parts.length == 1 ? BigInteger.ZERO : new BigInteger(parts[1]);
        power = power.add(BigInteger.valueOf((long) digits.length() - end - fraction));
        return new ExactDecimal(mantissa.startsWith("-") ? value.negate() : value, power);
    }

    static final class Sum {
        private final TreeMap<BigInteger, BigInteger> terms = new TreeMap<>();

        void add(ExactDecimal price, BigInteger quantity) {
            terms.merge(price.exponent, price.coefficient.multiply(quantity), BigInteger::add);
        }

        boolean matches(ExactDecimal expected) {
            var difference = new TreeMap<>(terms);
            difference.merge(expected.exponent, expected.coefficient.negate(), BigInteger::add);
            BigInteger carry = BigInteger.ZERO;
            BigInteger previousPower = null;
            for (var term : difference.entrySet()) {
                if (carry.signum() != 0) {
                    // Higher-place terms can cancel a residual only if its low digits are zero.
                    // Compare with the available digits without expanding the exponent gap.
                    BigInteger gap = term.getKey().subtract(previousPower);
                    String digits = carry.toString();
                    int remaining = digits.length();
                    while (digits.charAt(remaining - 1) == '0') remaining--;
                    if (gap.compareTo(BigInteger.valueOf(digits.length() - remaining)) > 0) return false;
                    carry = new BigInteger(digits.substring(0, digits.length() - gap.intValueExact()));
                }
                carry = carry.add(term.getValue());
                previousPower = term.getKey();
            }
            return carry.signum() == 0;
        }
    }
}
