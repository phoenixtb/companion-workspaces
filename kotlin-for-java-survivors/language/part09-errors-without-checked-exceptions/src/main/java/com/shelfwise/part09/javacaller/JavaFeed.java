package com.shelfwise.part09.javacaller;

import com.shelfwise.part09.interop.CheckedPriceSource;

import java.io.IOException;
import java.util.List;

// Implementing a Kotlin interface from Java. Against PriceSource (no @Throws), this fails:
//   error: load() in JavaFeed cannot implement load() in PriceSource
//     overridden method does not throw IOException
public final class JavaFeed implements CheckedPriceSource {
    @Override
    public List<String> load() throws IOException {
        throw new IOException("export locked");
    }
}
