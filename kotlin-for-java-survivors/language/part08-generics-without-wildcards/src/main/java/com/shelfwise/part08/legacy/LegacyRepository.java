package com.shelfwise.part08.legacy;

import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

// The shared data-access library's signature, wildcards at every use site (PECS).
public interface LegacyRepository<T extends Entity<ID>, ID extends Comparable<? super ID>> {
    List<? extends T> findAll(Predicate<? super T> filter, Comparator<? super T> order);
}
