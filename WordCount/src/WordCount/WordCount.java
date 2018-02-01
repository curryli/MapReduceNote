package WordCount;
import java.io.IOException;
import java.util.StringTokenizer;

 


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
    //输入key值类型（这里map的key是行号，类型Object）、输入value值类型、输出key值类型和输出value值类型。
    public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{
        private final static IntWritable one=new IntWritable(1);
        private Text word = new Text();
        @Override
	//Object key,输入key、Text value输入value,  map reduce 共享context
	public void map(Object key,Text value,Context context) throws IOException,InterruptedException{  
            StringTokenizer itr=new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

  //输入key值类型（这里reduce的key是上面输出的key，即单词，类型Text）、输入value值类型、输出key值类型和输出value值类型。
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
	//Text key,输入key、Iterable<IntWritable> values输入value
	//这里reduce函数输入值类型是Iterable<IntWritable>，是因为在map阶段根据key，把相同key的值组成了一个集合，传给reducer处理
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	  Configuration conf = new Configuration();
    	  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	  if (otherArgs.length != 2) {
    	    System.err.println("Usage: WordCount <in> <out>");
    	    System.exit(2);
    	  }
    	  Job job = new Job(conf, "word count");
    	 
    	  job.setJarByClass(WordCount.class);
    	  job.setMapperClass(TokenizerMapper.class);
    	  job.setCombinerClass(IntSumReducer.class);
    	  job.setReducerClass(IntSumReducer.class);
    	  job.setOutputKeyClass(Text.class);
    	  job.setOutputValueClass(IntWritable.class);
    	  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    	  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    	  System.exit(job.waitForCompletion(true) ? 0 : 1);
    	}

}
