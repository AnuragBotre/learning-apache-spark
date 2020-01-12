package com.trendcore.learning.apache.spark;

import java.util.Arrays;
import java.util.HashMap;

public class Main {

    public static void main(String[] args) {
        HashMap<Integer, Integer> reduce = Arrays.asList(1, 1, 2, 3, 5, 2, 3, 4, 5).stream()
                .map(integer -> integer.intValue())
                .reduce(new HashMap<>(), (hashMap, integer) -> {
                    hashMap.computeIfAbsent(integer, o -> 0);
                    hashMap.compute(integer, (o, o2) -> o2 + 1);
                    System.out.println("accumulating");
                    return hashMap;
                }, (hashMap, hashMap2) -> {
                    //combiner is called only during in case of parallel stream.
                    hashMap.putAll(hashMap2);
                    System.out.println("combining");
                    return hashMap;
                });

        reduce.entrySet().stream().forEach(o -> {
            System.out.println(o.getKey() + " " + o.getValue());
        });
    }

}
