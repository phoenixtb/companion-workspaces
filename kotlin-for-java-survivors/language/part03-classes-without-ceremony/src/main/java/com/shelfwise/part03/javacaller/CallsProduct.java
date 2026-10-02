package com.shelfwise.part03.javacaller;

import com.shelfwise.part03.story.Product;

/** Java sees Kotlin properties as ordinary getters and setters. */
public class CallsProduct {
    public static void main(String[] args) {
        Product product = new Product("Toor Dal 1kg", 16_500);
        product.setPricePaise(15_900);
        System.out.println(product.getName() + " " + product.getPricePaise()); // -> Toor Dal 1kg 15900
    }
}
