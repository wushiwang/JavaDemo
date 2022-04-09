package com.wu.mapreduce.combiner;

import com.wu.mapreduce.wordcount.WordCountMapper;
import com.wu.mapreduce.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(); // 1 获取配置信息以及获取job对象
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountDriver.class);  // 2 关联本Driver程序的jar

        job.setMapperClass(WordCountMapper.class);  // 3 关联Mapper和Reducer的jar
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);  // 4 设置Mapper输出的kv类型
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);  // 5 设置最终输出kv类型
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(WordCountReducer.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\input\\inputword"));// 6 设置输入路径和输出路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output222"));

        boolean result = job.waitForCompletion(true);  // 7 提交job
        System.exit(result ? 0 : 1);
    }
}
