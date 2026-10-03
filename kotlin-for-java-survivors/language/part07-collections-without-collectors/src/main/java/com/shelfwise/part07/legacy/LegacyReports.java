package com.shelfwise.part07.legacy;

import java.util.Collections;
import java.util.List;

// The store-operations team's utility: sorts whatever list it is given, in place.
public final class LegacyReports {
    public static void sortInPlace(List<String> skus) {
        Collections.sort(skus);
    }
}
