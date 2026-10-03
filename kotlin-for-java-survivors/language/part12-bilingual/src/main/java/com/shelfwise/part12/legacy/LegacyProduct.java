package com.shelfwise.part12.legacy;

// A JavaBean from the old catalog library.
public class LegacyProduct {
    private String sku;
    private long pricePaise;
    private boolean active = true;
    private String supplierCode; // may be null; nothing says so
    private PriceListener listener = (oldPaise, newPaise) -> { };

    public interface PriceListener {
        void changed(long oldPaise, long newPaise);
    }

    public LegacyProduct(String sku, long pricePaise) {
        this.sku = sku;
        this.pricePaise = pricePaise;
    }

    public String getSku() { return sku; }
    public long getPricePaise() { return pricePaise; }
    public void setPricePaise(long pricePaise) {
        long old = this.pricePaise;
        this.pricePaise = pricePaise;
        listener.changed(old, pricePaise);
    }

    public void onPriceChange(PriceListener listener) { this.listener = listener; }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    public String getSupplierCode() { return supplierCode; }

    public static LegacyProduct sample() { return new LegacyProduct("SHW-1001", 16_500); }
}
