package com.supplier.sdk.v2;

import org.jspecify.annotations.Nullable;

public class SupplierItemV2 {
    private final String code;
    private final @Nullable String description;

    public SupplierItemV2(String code, @Nullable String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public @Nullable String getDescription() {
        return description;
    }
}
