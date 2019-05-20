import java.io.{InputStream, OutputStream}

import WordCount0.{SimpleDoFn, input}
import com.esotericsoftware.kryo.Kryo
import com.twitter.algebird.Semigroup
import com.twitter.chill.{Input, KryoSerializer, Output}
import org.apache.beam.sdk.coders._
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.util.EmptyOnDeserializationThreadLocal
import org.apache.beam.sdk.values.{KV, PCollection, TupleTag}
import org.apache.beam.sdk.{Pipeline, PipelineResult}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object WordCount14 {

  class ScioContext(val args: Array[String]) {
    val options = PipelineOptionsFactory.fromArgs(args: _*).create()
    val pipeline = Pipeline.create(options)

    pipeline.getCoderRegistry.registerCoderForClass(classOf[Int], VarIntCoder.of())
    pipeline.getCoderRegistry.registerCoderForClass(classOf[Long], VarLongCoder.of())

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

  implicit class PairSCollection[K: ClassTag, V: ClassTag](val self: SCollection[(K, V)]) {
    val toKV = {
      val kv = self.internal.apply(ParDo.of(new SimpleDoFn[(K, V), KV[K, V]]("toKV") {
        override def process(c: DoFn[(K, V), KV[K, V]]#ProcessContext) = {
          val kv = c.element()
          c.output(KV.of(kv._1, kv._2))
        }
      }))

      val r = self.internal.getPipeline.getCoderRegistry
      val kCls = implicitly[ClassTag[K]].runtimeClass.asInstanceOf[Class[K]]
      val vCls = implicitly[ClassTag[V]].runtimeClass.asInstanceOf[Class[V]]
      kv.setCoder(KvCoder.of(r.getCoder(kCls), r.getCoder(vCls)))
      new SCollection(kv)
    }

    def groupByKey: SCollection[(K, Iterable[V])] = toKV
      .applyTransform(GroupByKey.create())
      .map(kv => (kv.getKey, kv.getValue.asScala))

    def reduceByKey(f: (V, V) => V) = toKV
      .applyTransform(Combine.perKey(new BinaryCombineFn[V] {
        override def apply(left: V, right: V) = f(left, right)
      }))
      .map(kv => (kv.getKey, kv.getValue))

    def sumByKey(implicit sg: Semigroup[V]) = reduceByKey(sg.plus)

    def cogroup[W: ClassTag](that: SCollection[(K, W)]): SCollection[(K, (Iterable[V], Iterable[W]))] = {
      val vTag = new TupleTag[V]()
      val wTag = new TupleTag[W]()
      val grouped = KeyedPCollectionTuple
        .of(vTag, this.toKV.internal)
        .and(wTag, new PairSCollection(that).toKV.internal)
        .apply(CoGroupByKey.create())
      new SCollection(grouped).map { kv =>
        val vs = kv.getValue.getAll(vTag).asScala
        val ws = kv.getValue.getAll(wTag).asScala
        (kv.getKey, (vs, ws))
      }
    }
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

  val meanings = Seq("du" -> "you", "hast" -> "hate", "hast" -> "have", "mich" -> "me")
  val expected = Seq("du 7 you", "hast 6 have", "hast 6 hate", "mich 5 me")

  def main(args: Array[String]): Unit = {
    val sc = ScioContext(args)
    val wordCount = sc.parallelize(input.asScala)
      .flatMap(_.toLowerCase().split("[^\\p{L}]+"))
      .filter(_.nonEmpty)
      .countByValue
    val that = sc.parallelize(meanings)

    val result = wordCount.cogroup(that)
      .flatMap { case (k, (lhs, rhs)) =>
        for (l <- lhs; r <- rhs) yield (k, (l, r))
      }
      .map(kv => kv._1 + " " + kv._2._1 + " " + kv._2._2)

    PAssert.that(result.internal).containsInAnyOrder(expected.asJava)

    sc.close()
  }
}
// Challenge: implement join