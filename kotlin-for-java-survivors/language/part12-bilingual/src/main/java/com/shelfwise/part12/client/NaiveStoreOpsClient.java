package com.shelfwise.part12.client;

import com.shelfwise.part12.naive.PriceList;
import com.shelfwise.part12.naive.PriceListNaiveKt;
import com.shelfwise.part12.naive.Rounding;

// What the store-operations team had to write against the first version.
public final class NaiveStoreOpsClient {
    public static void main(String[] args) {
        PriceList list = PriceList.Companion.forCurrency("INR"); // a method on the companion object
        PriceList explicit = new PriceList("INR", 100); // default arguments don't exist in Java
        list.set("SHW-1001", 16_550);
        System.out.println(list.priceOf("SHW-1001", 0)); // -> 16500
        System.out.println(PriceList.Companion.getSUPPORTED_CURRENCIES()); // -> [INR, NPR]
        System.out.println(Rounding.INSTANCE.toRupee(14_850)); // -> 14800
        System.out.println(PriceListNaiveKt.formatPaise(14_850)); // -> ₹148.50
        System.out.println(explicit.getCurrency()); // -> INR
        // listOf(...) is backed by Arrays.asList: add is refused, set is not. A shared constant just changed.
        PriceList.Companion.getSUPPORTED_CURRENCIES().set(1, "USD");
        System.out.println(PriceList.Companion.getSUPPORTED_CURRENCIES()); // -> [INR, USD]
        // shelfLabel compiles to shelfLabel-<hash>(String, long): not a legal Java identifier, so no call is possible.
    }
}
