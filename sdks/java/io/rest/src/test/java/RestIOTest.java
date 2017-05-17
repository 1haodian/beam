import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.rest.RestIO;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;

public class RestIOTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test @Category(NeedsRunner.class) public void tesRestIO() throws Exception {
    PCollection<String> output = pipeline.apply(RestIO.<KV<String, String>>read()
        .withLocation("http://jqhadoop-test28-33.int.yihaodian.com:8088/ws/v1/cluster/"))
        .setCoder(StringUtf8Coder.of());
    PCollection<KV<String, String>> output1 = output.apply(ParDo.of(new ExtractResult()));
    RestIO.write().expand(output1);
    pipeline.run();
  }

  private static class ExtractResult extends DoFn<String, KV<String, String>> {

    @DoFn.ProcessElement public void processElement(ProcessContext c) {
      String log = c.element();
      Assert.assertTrue(log.endsWith("\"hadoopVersionBuiltOn\":\"2015-06-25T02:34Z\"}}"));
      c.output(KV.of("http://jqhadoop-test28-33.int.yihaodian.com:8088/ws/v1",log));
    }
  }
}
