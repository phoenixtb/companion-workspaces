package com.shelfwise.part06.javacaller;

import com.shelfwise.part06.dsl.Product;
import com.shelfwise.part06.dsl.ProductQuery;
import com.shelfwise.part06.dsl.ProductQueryKt;
import com.shelfwise.part06.extensions.ExtensionsKt;
import kotlin.Unit;

import java.util.List;

// What the store-operations team's Java batch job sees of this part's Kotlin API.
public final class FromJava {
    public static void main(String[] args) {
        // An extension is a static method on the file's facade class, receiver first.
        System.out.println(ExtensionsKt.isSku("SHW-1001")); // -> true

        // A DSL entry point takes a Function1<QueryBuilder, Unit>: usable, but no longer a DSL.
        ProductQuery pasta = ProductQueryKt.query(q -> {
            q.category("pasta");
            q.limit(1);
            return Unit.INSTANCE;
        });
        List<Product> found = pasta.invoke(List.of(new Product("SHW-4002", "Fusilli 500g", "pasta", 9_500, java.util.Set.of())));
        System.out.println(found.get(0).getName()); // -> Fusilli 500g
    }
}
