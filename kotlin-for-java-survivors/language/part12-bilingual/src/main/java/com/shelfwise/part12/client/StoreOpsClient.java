package com.shelfwise.part12.client;

import com.shelfwise.part12.api.PriceList;
import com.shelfwise.part12.api.PriceTag;
import com.shelfwise.part12.api.QuoteService;
import com.shelfwise.part12.api.Pricing;
import com.shelfwise.part12.api.Rounding;
import com.shelfwise.part12.api.Sku;

// The store-operations team's Java code, using the annotated library.
public final class StoreOpsClient {
    public static void main(String[] args) {
        PriceList list = PriceList.forCurrency("INR"); // @JvmStatic
        PriceList defaults = new PriceList(); // @JvmOverloads constructor
        list.set("SHW-1001", 16_550);
        System.out.println(list.priceOf("SHW-1001")); // -> 16500
        System.out.println(list.priceOf("SHW-1001", 10)); // -> 14800
        // Kotlin checks non-null parameters on entry: a null from Java fails at the boundary, by name.
        try {
            list.priceOf(null);
        } catch (NullPointerException e) {
            System.out.println(e.getMessage());
            // -> Parameter specified as non-null is null: method com.shelfwise.part12.api.PriceList.priceOf, parameter sku
        }
        System.out.println(PriceList.DEFAULT_CURRENCY + " " + PriceList.SUPPORTED_CURRENCIES + " " + defaults.getCurrency()); // -> INR [INR, NPR] INR

        // @JvmRecord: a real record, so record patterns work.
        Object tag = new PriceTag("SHW-1001", 14_800);
        if (tag instanceof PriceTag(String sku, long paise)) {
            System.out.println(sku + " at " + Pricing.formatPaise(paise)); // -> SHW-1001 at ₹148.00
        }
        System.out.println(Rounding.toRupee(14_850)); // -> 14800

        // Value classes: the boxed API validates; the @JvmName one does not.
        System.out.println(Pricing.shelfLabel(new Sku("SHW-1001"), 14_800)); // -> SHW-1001 ₹148
        System.out.println(Pricing.shelfLabelUnchecked("not a sku", 14_800)); // -> not a sku ₹148
        try {
            new Sku("not a sku");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage()); // -> bad SKU: not a sku
        }

        // The constant is wrapped in Collections.unmodifiableList: set is refused too.
        try {
            PriceList.SUPPORTED_CURRENCIES.set(1, "USD");
        } catch (UnsupportedOperationException e) {
            System.out.println("set refused"); // -> set refused
        }

        // A suspend function is Object bestQuote(String, Continuation) to Java; the future-returning twin isn't.
        try (QuoteService quotes = new QuoteService()) {
            System.out.println(quotes.bestQuoteAsync("SHW-1001").join()); // -> 16000
        }
    }
}
