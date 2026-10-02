package com.shelfwise.part01.story

// Kabir's first pull request: Java, transliterated. It compiles, semicolons and all.
class PriceUtils {
    companion object {
        @JvmStatic
        fun format(paise: Long): String {
            var result: String;
            if (paise < 0) {
                result = "-";
            } else {
                result = "₹" + paise / 100 + "." + String.format("%02d", paise % 100);
            }
            return result;
        }
    }
}

fun main() {
    println(PriceUtils.format(19_900)) // -> ₹199.00
}
