package com.shopping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class GenderCategoryPreference extends Configured implements Tool {

    /**
     * Mapper类：按性别和产品类别分组，输出消费金额
     * 输入：原始购物数据
     * 输出：性别-产品类别 -> 单次消费金额
     */
    public static class GenderCategoryPreferenceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        Text genderCategoryKey = new Text();
        DoubleWritable amountValue = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 跳过标题行
            if (key.get() == 0 && value.toString().contains("invoice_no")) {
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length >= 7) {
                try {
                    String gender = fields[2].trim();          // 性别
                    String category = fields[4].trim();        // 产品类别
                    int quantity = Integer.parseInt(fields[5].trim());   // 数量
                    double price = Double.parseDouble(fields[6].trim()); // 单价

                    // 计算单次消费金额
                    double amount = quantity * price;

                    // 构建复合键：性别-产品类别
                    String compositeKey = gender + "\t" + category;
                    genderCategoryKey.set(compositeKey);
                    amountValue.set(amount);

                    context.write(genderCategoryKey, amountValue);

                } catch (NumberFormatException e) {
                    // 忽略格式错误的数据行
                    System.err.println("数据格式错误，跳过该行: " + value.toString());
                }
            }
        }
    }

    /**
     * Reducer类：计算每个性别-产品类别组合的总金额和平均金额
     * 输出：性别-产品类别 -> 总金额 平均金额 交易次数
     */
    public static class GenderCategoryPreferenceReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalAmount = 0.0;      // 总消费金额
            int transactionCount = 0;      // 交易次数
            double minAmount = Double.MAX_VALUE; // 最小消费金额
            double maxAmount = 0.0;        // 最大消费金额

            // 计算总金额和交易次数
            for (DoubleWritable value : values) {
                double amount = value.get();
                totalAmount += amount;
                transactionCount++;

                // 更新最小和最大金额
                if (amount < minAmount) {
                    minAmount = amount;
                }
                if (amount > maxAmount) {
                    maxAmount = amount;
                }
            }

            // 计算平均单次消费金额
            double averageAmount = transactionCount > 0 ? totalAmount / transactionCount : 0.0;

            // 格式化输出：总金额 平均金额 交易次数 最小金额 最大金额
            String output = String.format("%.2f\t%.2f\t%d\t%.2f\t%.2f",
                    totalAmount, averageAmount, transactionCount, minAmount, maxAmount);
            result.set(output);
            context.write(key, result);
        }
    }

    public static void main(String[] args) {
        // 设置默认输入输出路径
        String[] myArgs = {
                "/ShoppingDataAnalysis/customer_shopping_data.csv",
                "/ShoppingDataAnalysis/GenderCategoryPreferenceOutput"
        };

        try {
            int exitCode = ToolRunner.run(new Configuration(), new GenderCategoryPreference(), myArgs);
            System.exit(exitCode);
        } catch (Exception e) {
            System.err.println("程序执行失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getMyConfiguration();

        Job job = Job.getInstance(conf, "Gender Category Preference Analysis");
        job.setJarByClass(GenderCategoryPreference.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(GenderCategoryPreferenceMapper.class);
        job.setReducerClass(GenderCategoryPreferenceReducer.class);

        // 设置输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 删除已存在的输出目录
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("已删除已存在的输出目录: " + args[1]);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        // 提交作业并等待完成
        boolean success = job.waitForCompletion(true);
        System.out.println("性别与产品类别消费偏好分析作业" + (success ? "完成" : "失败"));
        return success ? 0 : 1;
    }

    /**
     * 配置Hadoop环境
     */
    public static Configuration getMyConfiguration() {
        Configuration conf = new Configuration();
        // 新增的3行配置，如果没有出现HDFS权限拒绝错误可以不用添加以下三行
        String userPath = "/user/root";
        // 配置参数
        conf.set("hadoop.tmp.dir", userPath + "/tmp");
        conf.set("yarn.app.mapreduce.am.staging-dir", userPath + "/staging");
        conf.set("mapreduce.cluster.temp.dir", userPath + "/mapreduce/tmp");


        // 基本Hadoop配置
        conf.setBoolean("mapreduce.app-submission.cross-platform", true);
        conf.set("fs.defaultFS", "hdfs://shiye0:8020");
        conf.set("mapreduce.framework.name", "yarn");

        // YARN配置
        String resourcenode = "shiye1";
        conf.set("yarn.resourcemanager.address", resourcenode + ":8032");
        conf.set("yarn.resourcemanager.scheduler.address", resourcenode + ":8030");
        conf.set("mapreduce.jobhistory.address", resourcenode + ":10020");

        conf.set("mapreduce.job.jar", ShoppingDataAnalysisJarUtil.jar(GenderCategoryPreference.class));
        return conf;
    }
}