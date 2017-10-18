import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class WordCount0 {

  public static List<String> input = Lists.newArrayList(
      "Du",
      "Du hast",
      "Du hast mich",
      "Du hast mich",
      "Du hast mich gefragt",
      "Du hast mich gefragt",
      "Du hast mich gefragt und ich hab nichts gesagt");

  public static List<String> expected = Lists.newArrayList(
      "du 7",
      "hast 6",
      "mich 5",
      "gefragt 3",
      "und 1",
      "ich 1",
      "hab 1",
      "nichts 1",
      "gesagt 1");

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> result = pipeline
        // parallelize
        .apply(Create.of(input))
        // flatMap
        .apply(ParDo.of(new SimpleDoFn<String, String>("flatMap") {
          @Override
          public void process(ProcessContext c) {
            String line = c.element();
            for (String word : line.toLowerCase().split("[^\\p{L}]+")) {
              c.output(word);
            }
          }
        }))
        // filter
        .apply(ParDo.of(new SimpleDoFn<String, String>("filter") {
          @Override
          public void process(ProcessContext c) {
            String word = c.element();
            if (!word.isEmpty()) {
              c.output(word);
            }
          }
        }))
        // countByValue
        .apply(Count.perElement())
        // map
        .apply(ParDo.of(new SimpleDoFn<KV<String, Long>, String>("map") {
          @Override
          public void process(ProcessContext c) {
            KV<String, Long> kv = c.element();
            String word = kv.getKey();
            Long count = kv.getValue();
            c.output(word + " " + count);
          }
        }));

    PAssert.that(result).containsInAnyOrder(expected);

    pipeline.run().waitUntilFinish();
  }

  public static abstract class SimpleDoFn<A, B> extends DoFn<A, B> {

    private String name;

    public SimpleDoFn(String name) {
      this.name = name;
    }

    public abstract void process(DoFn<A, B>.ProcessContext c);

    private void log(String step) {
      System.out.println(
          String.format("%s %s %s thread: %d", name, step, this, Thread.currentThread().getId()));
    }

    @Setup
    public void setup() {
      log("Setup");
    }

    @StartBundle
    public void startBundle() {
      log("StartBundle");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      log("ProcessElement");
      process(c);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      log("FinishBundle");
    }

    @Teardown
    public void teardown() {
      log("Teardown");
    }
  }
}
// Challenge: rewrite in Scala