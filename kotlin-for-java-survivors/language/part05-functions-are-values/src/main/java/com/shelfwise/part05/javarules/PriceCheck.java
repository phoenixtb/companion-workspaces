package com.shelfwise.part05.javarules;

/** A Java functional interface, as found in any Java library. */
@FunctionalInterface
public interface PriceCheck {
    boolean accepts(long pricePaise);
}
