package com.shopping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class GenderCategoryTop5 extends Configured implements Tool {

    public static class GenderCategoryTop5Mapper
            extends Mapper<LongWritable, Text, GenderCategoryTop5Bean, NullWritable> {
        private GenderCategoryTop5Bean gcBean = new GenderCategoryTop5Bean();

        enum GenderCounter {
            MALE_COUNT,
            FEMALE_COUNT
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] reads = value.toString().trim().split("\t");
            // 输入格式：性别\t产品类别    总金额    平均金额    交易次数    最小金额    最大金额
            if (reads.length >= 6) {
                try {
                    String gender = reads[0];
                    String category = reads[1];
                    double totalAmount = Double.parseDouble(reads[2]);

                    // 更新计数器
                    if (gender.equals("Male")) {
                        context.getCounter(GenderCounter.MALE_COUNT).increment(1);
                    } else if (gender.equals("Female")) {
                        context.getCounter(GenderCounter.FEMALE_COUNT).increment(1);
                    }

                    gcBean.setGender(gender);
                    gcBean.setCategory(category);
                    gcBean.setTotalAmount(totalAmount);
                    context.write(gcBean, NullWritable.get());

                } catch (NumberFormatException e) {
                    System.err.println("数据格式错误: " + value.toString());
                }
            }
        }
    }

    /**
     * Combiner类：在Map端进行局部聚合
     */
    public static class GenderCategoryTop5Combiner
            extends Reducer<GenderCategoryTop5Bean, NullWritable, GenderCategoryTop5Bean, NullWritable> {

        @Override
        protected void reduce(GenderCategoryTop5Bean key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            // 直接输出，Combiner主要用于优化
            context.write(key, NullWritable.get());
        }
    }

    /**
     * Partitioner类：按性别分区
     */
    public static class GenderCategoryTop5Partitioner
            extends org.apache.hadoop.mapreduce.Partitioner<GenderCategoryTop5Bean, NullWritable> {

        @Override
        public int getPartition(GenderCategoryTop5Bean key, NullWritable value, int numPartitions) {
            // 根据性别分区：Male -> 0, Female -> 1
            if (key.getGender().equals("Male")) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    public static class GenderCategoryTop5Reducer
            extends Reducer<GenderCategoryTop5Bean, NullWritable, Text, NullWritable> {
        private Text resultKey = new Text();
        private int count = 0;
        private String currentGender = "";

        enum OutputCounter {
            MALE_OUTPUT,
            FEMALE_OUTPUT
        }

        @Override
        protected void reduce(GenderCategoryTop5Bean key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            // 如果是新的性别，重置计数器
            if (!key.getGender().equals(currentGender)) {
                currentGender = key.getGender();
                count = 0;
            }

            // 只输出前5条记录
            if (count < 5) {
                for (NullWritable value : values) {
                    if (count < 5) {
                        // 输出格式：性别\t产品类别\t总金额
                        String output = key.getGender() + "\t" + key.getCategory() + "\t" + String.format("%.2f", key.getTotalAmount());
                        resultKey.set(output);
                        context.write(resultKey, NullWritable.get());

                        // 更新计数器
                        if (key.getGender().equals("Male")) {
                            context.getCounter(OutputCounter.MALE_OUTPUT).increment(1);
                        } else if (key.getGender().equals("Female")) {
                            context.getCounter(OutputCounter.FEMALE_OUTPUT).increment(1);
                        }

                        count++;
                    } else {
                        break;
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("性别 " + currentGender + " 输出记录数: " + count);
        }
    }

    public static void main(String[] args) {
        String[] myArgs = {
                "/ShoppingDataAnalysis/GenderCategoryPreferenceOutput",
                "/ShoppingDataAnalysis/GenderCategoryTop5Output"
        };
        try {
            ToolRunner.run(new Configuration(), new GenderCategoryTop5(), myArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getMyConfiguration();
        Job job = Job.getInstance(conf, "Gender Category Top5 Analysis");
        job.setJarByClass(GenderCategoryTop5.class);
        job.setMapperClass(GenderCategoryTop5Mapper.class);
        job.setReducerClass(GenderCategoryTop5Reducer.class);
        job.setCombinerClass(GenderCategoryTop5Combiner.class);
        job.setPartitionerClass(GenderCategoryTop5Partitioner.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(GenderCategoryTop5Bean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(conf).delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static Configuration getMyConfiguration() {
        Configuration conf = new Configuration();

        // 配置参数
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
        conf.set("mapreduce.job.jar", ShoppingDataAnalysisJarUtil.jar(GenderCategoryTop5.class));

        return conf;
    }
}