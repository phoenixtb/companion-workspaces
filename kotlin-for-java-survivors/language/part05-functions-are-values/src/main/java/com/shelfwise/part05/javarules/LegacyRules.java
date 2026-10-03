package com.shelfwise.part05.javarules;

import com.shelfwise.part05.rules.Line;
import com.shelfwise.part05.rules.PriceRule;
import com.shelfwise.part05.rules.PricingRulesKt;
import kotlin.Unit;

import java.util.List;

// The Java pricing team's side: Java lambdas against the Kotlin API.
public final class LegacyRules {
    // A Kotlin fun interface: an ordinary functional interface to javac.
    public static final PriceRule STAFF_PRICE = line -> line.getPricePaise() * 80 / 100;

    public static void main(String[] args) {
        List<Line> lines = List.of(new Line("SHW-1001", "staples", 16_550, 1));
        // A Kotlin (String) -> Unit is a Function1<String, Unit>: the lambda must return Unit.INSTANCE.
        long total = PricingRulesKt.priceBasket(lines, STAFF_PRICE, message -> {
            System.out.println("audit " + message);
            return Unit.INSTANCE;
        });
        System.out.println(total);
        // -> audit SHW-1001 -> 13240
        // -> 13240
    }
}
