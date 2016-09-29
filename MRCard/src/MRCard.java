import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

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

public class MRCard {
    //map将输入中的value复制到输出数据的key上，并直接输出
    public static class Map extends Mapper<Object,Text,Text,Text>{
        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException{
            if(value!=null && !value.equals("")){
        	String[] paraArray = value.toString().split("\\001");
            context.write(new Text(paraArray[0]), new Text(paraArray[1]));
            }
        }
    }
 
    //reduce将输入中的key复制到输出数据的key上，并直接输出
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        //实现reduce函数
    	private Text amountListText = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	StringBuilder amountListString = new StringBuilder(); 
        	for (Text val : values) {
        		amountListString.append(val.toString()+",");
             }
        	
        	amountListString.deleteCharAt(amountListString.length()-1);
        	
        	amountListText.set(amountListString.toString());
            context.write(key, amountListText);
        }
    }
        
        
    public static void main(String[] args) throws Exception {
  	  Configuration conf = new Configuration();
  	  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  	  //GenericOptionsParser这个类，它的作用是将命令行中参数自动设置到变量conf中。很轻松就可以将参数与代码分离开。
  	  if (otherArgs.length != 2) {
  	    System.err.println("Usage: Card amount list MR <in> <out>");
  	    System.exit(2);
  	  }
  	 Job job = new Job(conf, "Card amount list MR");
  	 
  	 job.setJarByClass(MRCard. class );
  	 job.setMapperClass(Map. class );
//    job.setCombinerClass(Reduce. class );
     job.setReducerClass(Reduce. class );
     job.setOutputKeyClass(Text. class );
     job.setOutputValueClass(Text. class );
  	  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
  	  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  	  System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}


}

/*
输入 
其实是^A，也就是hive默认的"\\001"
卡号^A金额^A其他

0dd642f3e32ff858044133c6e607315098500101084929
0dd642f3e32ff858044133c6e607315049250101085113
0dd642f3e32ff858044133c6e607315098500101085310
0dd642f3e32ff858044133c6e607315098500101090626
0dd642f3e32ff858044133c6e607315049250101092010
0dd642f3e32ff858044133c6e607315049250101094148
0dd642f3e32ff858044133c6e607315049250101095646
0dd642f3e32ff858044133c6e607315019700101100628
0dd642f3e32ff858044133c6e607315019700101100744
0dd642f3e32ff858044133c6e607315049250101101416
0dd642f3e32ff858044133c6e607315019700101102009
0dd642f3e32ff858044133c6e607315029550101102239
0dd642f3e32ff858044133c6e607315029550101102423
0dd642f3e32ff858044133c6e607315098500101110840
0dd642f3e32ff858044133c6e607315049250101111114
0dd642f3e32ff858044133c6e607315098500101111457
0dd642f3e32ff858044133c6e607315098500101112008
0dd642f3e32ff858044133c6e607315098500101112156
0dd642f3e32ff858044133c6e607315019700101113710
0dd642f3e32ff858044133c6e607315029550101121927

输出   卡号^A金额1，金额2.。。。。金额N
0dd642f3e32ff858044133c6e6073150        2955,1970,9850,9850,9850,4925,9850,2955,2955,1970,4925,1970,1970,4925,4925,4925,9850,9850,4925,9850



运行命令 hadoop jar MRCard.jar MRCard -Dmapreduce.job.queuename=root.default xrli/MRCardFile.txt xrli/MRCardOut

*/