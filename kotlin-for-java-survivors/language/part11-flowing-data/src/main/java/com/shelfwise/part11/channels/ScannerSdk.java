package com.shelfwise.part11.channels;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

// A vendor's shelf-scanner SDK: listener-based, and the caller must unregister by hand.
public final class ScannerSdk {
    public interface ScanListener {
        void onScan(String sku, int units);

        void onError(Exception e);
    }

    private final List<ScanListener> listeners = new CopyOnWriteArrayList<>();

    public void addListener(ScanListener listener) { listeners.add(listener); }

    public void removeListener(ScanListener listener) { listeners.remove(listener); }

    public int listenerCount() { return listeners.size(); }

    // The real SDK calls listeners from its own I/O thread; the example calls this directly.
    public void deliver(String sku, int units) {
        for (ScanListener listener : listeners) listener.onScan(sku, units);
    }
}
