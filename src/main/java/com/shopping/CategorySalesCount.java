package com.shopping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CategorySalesCount extends Configured implements Tool {

    public static class CategorySalesCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text categoryKey = new Text();
        IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过标题行
            if (key.get() == 0 && value.toString().contains("invoice_no")) {
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length >= 5) {
                String category = fields[4].trim();  // 产品类别
                if (!category.isEmpty()) {
                    categoryKey.set(category);
                    context.write(categoryKey, one);
                }
            }
        }
    }

    public static class CategorySalesCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int transactionCount = 0;
            for (IntWritable value : values) {
                transactionCount++;
            }
            result.set(transactionCount);
            context.write(key, result);
        }
    }

    public static void main(String[] args) {
        String[] myArgs = {
                "/ShoppingDataAnalysis/customer_shopping_data.csv",
                "/ShoppingDataAnalysis/CategorySalesCountOutput"
        };
        try {
            ToolRunner.run(new Configuration(), new CategorySalesCount(), myArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getMyConfiguration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(CategorySalesCount.class);
        job.setMapperClass(CategorySalesCountMapper.class);
        job.setReducerClass(CategorySalesCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(conf).delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static Configuration getMyConfiguration() {
        Configuration conf = new Configuration();

        String userPath = "/user/root";
        // 配置参数
        conf.set("hadoop.tmp.dir", userPath + "/tmp");
        conf.set("yarn.app.mapreduce.am.staging-dir", userPath + "/staging");
        conf.set("mapreduce.cluster.temp.dir", userPath + "/mapreduce/tmp");

        conf.setBoolean("mapreduce.app-submission.cross-platform", true);
        conf.set("fs.defaultFS", "hdfs://shiye0:8020");
        conf.set("mapreduce.framework.name", "yarn");
        String resourcenode = "shiye1";
        conf.set("yarn.resourcemanager.address", resourcenode + ":8032");
        conf.set("yarn.resourcemanager.scheduler.address", resourcenode + ":8030");
        conf.set("mapreduce.jobhistory.address", resourcenode + ":10020");
        conf.set("mapreduce.job.jar", ShoppingDataAnalysisJarUtil.jar(CategorySalesCount.class));

        return conf;
    }
}