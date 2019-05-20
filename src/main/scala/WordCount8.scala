import java.io.{InputStream, OutputStream}

import WordCount0.{SimpleDoFn, expected, input}
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.{Input, KryoSerializer, Output}
import org.apache.beam.sdk.coders.{AtomicCoder, SerializableCoder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.util.EmptyOnDeserializationThreadLocal
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.{Pipeline, PipelineResult}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object WordCount8 {

  class ScioContext(val args: Array[String]) {
    val options = PipelineOptionsFactory.fromArgs(args: _*).create()
    val pipeline = Pipeline.create(options)
    def close(): PipelineResult = pipeline.run()

    def parallelize[A: ClassTag](elems: Iterable[A]) = {
      val p = pipeline.apply(Create.of(elems.asJava))
      new SCollection(p)
    }
  }

  object ScioContext {
    def apply(args: Array[String]): ScioContext = new ScioContext(args)
  }

  class SCollection[A: ClassTag](val internal: PCollection[A]) {
    val ct = implicitly[ClassTag[A]]

    def applyTransform[B: ClassTag](t: PTransform[PCollection[A], PCollection[B]]): SCollection[B] =
      new SCollection(internal.apply(t))

    def filter(f: A => Boolean): SCollection[A] = flatMap(x => if (f(x)) Some(x) else None)

    def map[B: ClassTag](f: A => B): SCollection[B] = flatMap(x => Some(f(x)))

    def flatMap[B: ClassTag](f: A => TraversableOnce[B]): SCollection[B] = {
      val p = internal.apply(ParDo.of(new SimpleDoFn[A, B]("flatMap") {
        override def process(c: DoFn[A, B]#ProcessContext) =
          f(c.element()).foreach(c.output)
      }))
      val cls = implicitly[ClassTag[B]].runtimeClass.asInstanceOf[Class[B]]
      val coder = internal.getPipeline.getCoderRegistry.getCoder(cls)
      if (coder.getClass != classOf[SerializableCoder[_]]) {
        p.setCoder(coder)
      } else {
        println(s"Using KryoAtomicCoder for $cls")
        p.setCoder(new KryoAtomicCoder[B])
      }
      new SCollection(p)
    }

    def countByValue: SCollection[(A, Long)] =
      applyTransform(Count.perElement()).map(kv => (kv.getKey, kv.getValue))
  }

  class KryoAtomicCoder[A] extends AtomicCoder[A] {
    private val kryo: ThreadLocal[Kryo] = new EmptyOnDeserializationThreadLocal[Kryo] {
      override def initialValue(): Kryo = KryoSerializer.registered.newKryo()
    }

    override def encode(value: A, outStream: OutputStream): Unit =
      kryo.get().writeClassAndObject(new Output(outStream), value)

    override def decode(inStream: InputStream): A =
      kryo.get().readClassAndObject(new Input(inStream)).asInstanceOf[A]
  }

  def main(args: Array[String]): Unit = {
    val sc = ScioContext(args)
    val result = sc.parallelize(input.asScala)
      .flatMap(_.toLowerCase().split("[^\\p{L}]+"))
      .filter(_.nonEmpty)
      .countByValue
      .map(kv => kv._1 + " " + kv._2)

    PAssert.that(result.internal).containsInAnyOrder(expected)

    sc.close()
  }
}
// Challenge: implement groupByKey