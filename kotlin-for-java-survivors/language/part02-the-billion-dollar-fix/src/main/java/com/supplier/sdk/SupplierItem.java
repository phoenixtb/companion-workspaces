package com.supplier.sdk;

import java.util.List;

/**
 * A supplier SDK class as it exists in the wild: no nullability annotations.
 * Kotlin sees every reference type here as a platform type (String!, List<String!>!).
 */
public class SupplierItem {
    private final String code;
    private final String description;
    private final List<String> allergens;

    public SupplierItem(String code, String description, List<String> allergens) {
        this.code = code;
        this.description = description;
        this.allergens = allergens;
    }

    public String getCode() {
        return code;
    }

    /** Documented nowhere: null for items the supplier hasn't catalogued yet. */
    public String getDescription() {
        return description;
    }

    public List<String> getAllergens() {
        return allergens;
    }
}
