import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import collection.JavaConverters._

object ScalaMapReduce extends Configured with Tool {

  def main(args: Array[String]): Unit = {
    val res: Int = ToolRunner.run(new Configuration(), this, args)
    System.exit(res)
  }

  class SwapMapper extends Mapper[Object, Text, Text, IntWritable] {
    val text = new Text()
    val one = new IntWritable(1)

    override def map(key: Object, value: Text,
                     context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val line = value.toString.trim().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s")
      for (word <- line) {
        text.set(word)
        context.write(text, one)
      }
    }
  }

  class SwapReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    val result = new IntWritable()

    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = 0
      for (res <- values.asScala) {
        sum += res.get()
      }
      result.set(sum)
      context.write(key, result)
    }
  }

  val IN_PATH_PARAM = "swap.input"
  val OUT_PATH_PARAM = "swap.output"

  override def run(args: Array[String]): Int = {
    val job = Job.getInstance(getConf, "Word Count")
    job.setJarByClass(getClass)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.setMapperClass(classOf[SwapMapper])
    job.setReducerClass(classOf[SwapReducer])
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


//docker cp target/scala-2.12/JVM_Hadoop_Scala-assembly-0.1.jar gbhdp:/home/hduser/

//hadoop jar JVM_Hadoop_Scala-assembly-0.1.jar \
//  -Dswap.input=/user/hduser/ppkm \
//  -Dswap.output=/user/hduser/ppkm_out