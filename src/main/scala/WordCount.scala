import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{Tool, ToolRunner}

import org.apache.hadoop.io.{IntWritable, LongWritable, Text};
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer};

import org.apache.hadoop.fs.{FileSystem, Path};
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException


object ScalaMapReduce extends Configured with Tool {

  val IN_PATH_PARAM = "swap.input"
  val OUT_PATH_PARAM = "swap.output"

  def main(args: Array[String]): Unit = {
    val res: Int = ToolRunner.run(new Configuration(), this, args)
    System.exit(res)
  }

  class SwapMapper extends Mapper[LongWritable, Text, IntWritable, Text] {
    val word = new Text()
    val amount = new IntWritable()

    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit = {
      val kv = value.toString.split("\\s+", 2)
      word.set(kv(0).trim().replaceAll("[^a-zA-Z ]", "").toLowerCase())
      amount.set(1)
      context.write(amount, word)
    }
  }

  class SwapReducer extends Reducer[LongWritable, Text, IntWritable, Text] {
    val word = new IntWritable()

    @throws[IOException]
    @throws[InterruptedException]
    def reduce(key: Nothing, values: Iterable[IntWritable], context: Nothing): Unit = {
      var sum = 0
      for (`val` <- values) {
        sum += `val`.get
      }
      result.set(sum)
      context.write(key, result)
    }
  }




  override def run(args: Array[String]): Int = {
    val job = Job.getInstance(getConf, "Word Count")
    job.setJarByClass(getClass)
    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[Text])
    job.setMapperClass(classOf[SwapMapper])
    job.setReducerClass(classOf[Reducer[IntWritable, Text, IntWritable, Text]])
    job.setNumReduceTasks(1)
    val in = new Path(getConf.get(IN_PATH_PARAM))
    val out = new Path(getConf.get(OUT_PATH_PARAM))
    FileInputFormat.addInputPath(job, in)
    FileOutputFormat.setOutputPath(job, out)
    val fs = FileSystem.get(getConf)
    if (fs.exists(out)) fs.delete(out, true)
    if (job.waitForCompletion(true)) 0 else 1
  }

}
