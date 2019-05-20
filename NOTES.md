# WordCount0
## Further Reading
- [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)
- [Beam Execution Model](https://beam.apache.org/documentation/execution-model/)
- [Scio PHP](https://www.lyh.me/slides/scio-php.html) - a joke talk about concurrency in Scio & Beam

# WordCount1
## Further Reading
- [Scala and Java Primitives Types](https://www.lyh.me/slides/primitives.html)

# WordCount2
## Further Reading
- [For Loop vs While](https://stackoverflow.com/questions/21373514/why-are-scala-for-loops-slower-than-logically-identical-while-loops) - why Scio uses `while` instead of `foreach` in `flatMap`

# WordCount3
## Further Reading
- Common coders in Beam `org.apache.beam.sdk.coders.CoderRegistry.CommonTypes`
- Methods in `org.apache.beam.sdk.coders.Coder`, especially `verifyDeterministic`, `consistentWithEquals`, `structuralValue`, `isRegisterByteSizeObserverCheap`, and `registerByteSizeObserver`
- Various Beam coder implementations

# WordCount4
## Exercises
- Rewrite `map` without `flatMap` and the temporary `Some`

# WordCount5
## Exercises
- Rewrite `filter` without `flatMap` and the temporary `Option`

# WordCount6
## Exercises
- What's the coder type in `map` in `countByValue` and why?

# WordCount7
## Exercises
- Implement a coder in for `(A, Long)` after the `map` in `countByValue` by delegating to `KvCoder` and `VarLongCoder`. Does it scale from a code maintenance perspective?

# WordCount8
## Further Reading
- [Twitter Chill](https://github.com/twitter/chill/tree/develop/chill-scala/src/main/scala/com/twitter/chill)

# WordCount9
## Further Reading
- [Scala Implicit Classes](https://docs.scala-lang.org/overviews/core/implicit-classes.html)
- [Nature of Iterables in GroupByKey Transform](https://stackoverflow.com/questions/46764654/nature-of-iterables-in-groupbykey-transform)
- [`KryoAtomicCoder#registerByteSizeObserver` inefficient for large iterables](https://github.com/spotify/scio/issues/476)

# WordCount10
## Exercises
- What happens when you `groupByKey` on an `SCollection[(String, KV[Long, Long])]`?

# WordCount11
## Further Reading
- `org.apache.beam.sdk.transforms.Combine.BinaryCombineFn` and `org.apache.beam.sdk.transforms.Combine.CombineFn`
## Exercises
- What's the performance implication if you `reduceByKey` when the value type is non-trivial, e.g. on a `SCollection[(String, Set[String])]`?

# WordCount12
## Further Reading
- [Scala Type Classes](https://www.lyh.me/slides/type-classes.html)
- [Semigroups](https://www.lyh.me/slides/semigroups.html)
- [BigDiffy](https://www.lyh.me/slides/bigdiffy.html) - an extreme case of Semigroup
## Exercises
- Implement a more efficient `sumByKey` by using `Semigroup#sumOption` and `org.apache.beam.sdk.transforms.Combine.CombineFn` with accumulator.

# WordCount13
## Further Reading
- [Scala For Comprehension](https://www.lyh.me/slides/for-yield.html)

# WordCount14
## Further Reading
- `org.apache.beam.sdk.transforms.join.CoGbkResult`

# WordCount15
## Further Reading
- [Scio Joins](https://www.lyh.me/slides/joins.html)
- [Sort-Merge Join](https://en.wikipedia.org/wiki/Sort-merge_join)
## Exercises
- Implement left, right, and outer join
