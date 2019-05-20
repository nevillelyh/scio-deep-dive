import WordCount0.{SimpleDoFn, expected, input}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.{Count, Create, DoFn, ParDo}
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.{Pipeline, PipelineResult}

import scala.collection.JavaConverters._

object WordCount1 {

  class ScioContext(val args: Array[String]) {
    val options = PipelineOptionsFactory.fromArgs(args: _*).create()
    val pipeline = Pipeline.create(options)
    def close(): PipelineResult = pipeline.run()

    def parallelize[A](elems: Iterable[A]) = pipeline.apply(Create.of(elems.asJava))
  }

  object ScioContext {
    def apply(args: Array[String]): ScioContext = new ScioContext(args)
  }

  def main(args: Array[String]): Unit = {
    val sc = ScioContext(args)
    val result = sc.parallelize(input.asScala)
      .apply(ParDo.of(new SimpleDoFn[String, String]("flatMap") {
        override def process(c: DoFn[String, String]#ProcessContext) =
          c.element().toLowerCase().split("[^\\p{L}]+").foreach(c.output)
      }))
      .apply(ParDo.of(new SimpleDoFn[String, String]("filter") {
        override def process(c: DoFn[String, String]#ProcessContext) = {
          val word = c.element()
          if (word.nonEmpty) c.output(word)
        }
      }))
      .apply(Count.perElement())
      .apply(ParDo.of(new SimpleDoFn[KV[String, java.lang.Long], String]("map") {
        override def process(c: DoFn[KV[String, java.lang.Long], String]#ProcessContext) = {
          val kv = c.element()
          val word = kv.getKey
          val count = kv.getValue
          c.output(word + " " + count)
        }
      }))

    PAssert.that(result).containsInAnyOrder(expected)

    sc.close()
  }
}
// Challenge: implement flatMap