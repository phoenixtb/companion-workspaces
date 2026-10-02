package com.supplier.sdk;

import com.shelfwise.part02.platform.SkuRulesKt;

/** Java code calling Kotlin with a null, the reverse direction of a platform type. */
public class LegacyCaller {
    public static void main(String[] args) {
        System.out.println(SkuRulesKt.normalise(" sup-7781 ")); // -> SUP-7781
        try {
            SkuRulesKt.normalise(null);
        } catch (NullPointerException e) {
            System.out.println(e.getMessage()); // -> Parameter specified as non-null is null: method com.shelfwise.part02.platform.SkuRulesKt.normalise, parameter sku
        }
    }
}
