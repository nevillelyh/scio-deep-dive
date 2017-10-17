import WordCount0.{SimpleDoFn, expected, input}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.{Pipeline, PipelineResult}

import scala.collection.JavaConverters._

object WordCount2 {

  class ScioContext(val args: Array[String]) {
    val options = PipelineOptionsFactory.fromArgs(args: _*)
    val pipeline = Pipeline.create()
    def close(): PipelineResult = pipeline.run()

    def parallelize[A](elems: Iterable[A]) = {
      val p = pipeline.apply(Create.of(elems.asJava))
      new SCollection(p)
    }
  }

  object ScioContext {
    def apply(args: Array[String]): ScioContext = new ScioContext(args)
  }

  class SCollection[A](val internal: PCollection[A]) {
    def applyTransform[B](t: PTransform[PCollection[A], PCollection[B]]): SCollection[B] =
      new SCollection(internal.apply(t))

    def flatMap[B](f: A => TraversableOnce[B]): SCollection[B] = {
      val p = internal.apply(ParDo.of(new SimpleDoFn[A, B]("flatMap") {
        override def process(c: DoFn[A, B]#ProcessContext) =
          f(c.element()).foreach(c.output)
      }))
      new SCollection(p)
    }
  }

  def main(args: Array[String]): Unit = {
    val sc = ScioContext(args)
    val result = sc.parallelize(input.asScala)
      .flatMap(_.toLowerCase().split("[^\\p{L}]+"))
      .flatMap(s => if (s.nonEmpty) Some(s) else None)
      .applyTransform(Count.perElement())
      .flatMap { kv =>
        Some(kv.getKey + " " + kv.getValue)
      }

    PAssert.that(result.internal).containsInAnyOrder(expected)

    sc.close()
  }
}
