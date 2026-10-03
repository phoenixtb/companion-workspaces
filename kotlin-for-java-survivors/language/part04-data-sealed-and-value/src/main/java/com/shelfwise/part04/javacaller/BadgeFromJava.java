package com.shelfwise.part04.javacaller;

import com.shelfwise.part04.sealed.StockLevel;

/** Java 21 sees a Kotlin sealed interface as sealed: this switch needs no default branch. */
public class BadgeFromJava {
    static String badge(StockLevel level) {
        return switch (level) {
            case StockLevel.InStock in -> "in stock: " + in.getUnits();
            case StockLevel.Low low -> "low: " + low.getUnits();
            case StockLevel.OutOfStock out -> "sold out";
            case StockLevel.Discontinued d -> "discontinued";
        };
    }

    public static void main(String[] args) {
        System.out.println(badge(new StockLevel.Low(4, 10))); // -> low: 4
        System.out.println(badge(StockLevel.OutOfStock.INSTANCE)); // -> sold out
    }
}
