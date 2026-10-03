package com.shelfwise.part05.tailrec

class Category(val name: String, val parent: Category? = null)

// The recursive call is the last thing the function does, so the compiler turns it into a loop.
tailrec fun root(category: Category): Category {
    val parent = category.parent ?: return category
    return root(parent)
}

// An accumulator moves the work before the call, which is what makes it a tail call.
tailrec fun path(category: Category?, acc: String = ""): String =
    if (category == null) acc
    else path(category.parent, if (acc.isEmpty()) category.name else "${category.name} > $acc")

// Not a tail call: the '1 +' runs after the recursive call returns. The compiler warns and keeps the recursion.
// tailrec fun depth(category: Category): Int = if (category.parent == null) 0 else 1 + depth(category.parent)
//   warning: a function is marked as tail-recursive but no tail calls are found.

fun main() {
    val dal = Category("Dal", Category("Staples", Category("Grocery")))
    println(root(dal).name) // -> Grocery
    println(path(dal)) // -> Grocery > Staples > Dal

    // 100,000 levels deep: plain recursion would need 100,000 stack frames.
    var deep = Category("level-0")
    for (i in 1..100_000) deep = Category("level-$i", deep)
    println(root(deep).name) // -> level-0
}
