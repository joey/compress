package com.cloudera;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings( { "deprecation" })
public class Compress {

  public static class Map extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, NullWritable> output, Reporter reporter)
        throws IOException {
      output.collect(value, NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Compress.class);
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();

    if (otherArgs.length != 3) {
      System.err.println("Usage: compress <in> <out> <codec>");
      List<String> codecs = new ArrayList<String>();
      for (Class<? extends CompressionCodec> clazz : CompressionCodecFactory.getCodecClasses(conf)) {
        codecs.add(clazz.newInstance().getDefaultExtension().replace(".", ""));
      }
      System.err.println("\tValid codecs: " + codecs.toString());
      System.exit(2);
    }
    conf.setJobName("compress");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);

    conf.setMapperClass(Map.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));
    FileOutputFormat.setCompressOutput(conf, true);
    CompressionCodecFactory codecs = new CompressionCodecFactory(conf);
    Class<? extends CompressionCodec> clazz = codecs.getCodec(new Path("file."+otherArgs[2])).getClass();
    FileOutputFormat.setOutputCompressorClass(conf, clazz);
    JobClient.runJob(conf);
    return;
  }
}
