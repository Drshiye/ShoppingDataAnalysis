package com.shopping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.HashSet;
import java.util.Set;

public class MallSalesCount extends Configured implements Tool {

    /**
     * Mapper类：解析购物数据，输出购物中心名称和交易信息
     */
    public static class MallSalesMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text mallName = new Text();
        Text transactionInfo = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 跳过标题行
            if (key.get() == 0 && value.toString().contains("invoice_no")) {
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length >= 10) {
                try {
                    String invoiceNo = fields[0].trim();          // 发票编号
                    String mall = fields[9].trim();               // 购物中心名称
                    int quantity = Integer.parseInt(fields[5].trim());  // 数量
                    double price = Double.parseDouble(fields[6].trim()); // 单价

                    // 计算单笔交易金额
                    double amount = quantity * price;

                    // 输出键值对：购物中心名称 -> 发票编号+金额
                    mallName.set(mall);
                    transactionInfo.set(invoiceNo + ":" + amount);
                    context.write(mallName, transactionInfo);

                } catch (NumberFormatException e) {
                    // 忽略格式错误的数据行
                    System.err.println("数据格式错误: " + value.toString());
                }
            }
        }
    }

    /**
     * Reducer类：计算每个购物中心的总销售额和交易次数
     */
    public static class MallSalesReducer extends Reducer<Text, Text, Text, Text> {
        Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double totalSales = 0.0;          // 总销售额
            Set<String> uniqueInvoices = new HashSet<>();  // 用于去重计数交易次数

            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts.length == 2) {
                    String invoiceNo = parts[0];
                    double amount = Double.parseDouble(parts[1]);

                    totalSales += amount;
                    uniqueInvoices.add(invoiceNo);  // 使用Set自动去重
                }
            }

            int transactionCount = uniqueInvoices.size();  // 交易次数（去重后）

            // 格式化输出：总销售额(保留2位小数) 交易次数
            String output = String.format("%.2f\t%d", totalSales, transactionCount);
            result.set(output);
            context.write(key, result);
        }
    }

    public static void main(String[] args) {
        // 设置默认输入输出路径
        String[] myArgs = {
                "/ShoppingDataAnalysis/customer_shopping_data.csv",
                "/ShoppingDataAnalysis/MallSalesCountOutput"
        };

        try {
            int exitCode = ToolRunner.run(new Configuration(), new MallSalesCount(), myArgs);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getMyConfiguration();

        Job job = Job.getInstance(conf, "Mall Sales Analysis");
        job.setJarByClass(MallSalesCount.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(MallSalesMapper.class);
        job.setReducerClass(MallSalesReducer.class);

        // 设置输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 删除已存在的输出目录
        FileSystem.get(conf).delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业并等待完成
        return job.waitForCompletion(true) ? -1 : 1;
    }

    /**
     * 配置Hadoop环境
     */
    public static Configuration getMyConfiguration() {
        Configuration conf = new Configuration();

        String userPath = "/user/root";
        // 配置参数
        conf.set("hadoop.tmp.dir", userPath + "/tmp");
        conf.set("yarn.app.mapreduce.am.staging-dir", userPath + "/staging");
        conf.set("mapreduce.cluster.temp.dir", userPath + "/mapreduce/tmp");


        conf.setBoolean("mapreduce.app-submission.cross-platform", true);
        // HDFS配置
        conf.set("fs.defaultFS", "hdfs://shiye0:8020");
        // 指定使用yarn框架
        conf.set("mapreduce.framework.name", "yarn");


        // YARN配置
        String resourcenode = "shiye1";
        conf.set("yarn.resourcemanager.address", resourcenode + ":8032");//ResourceManager的地址配置
        // 指定资源分配器
        conf.set("yarn.resourcemanager.scheduler.address", resourcenode + ":8030");//ResourceManager的调度器地址配置
        conf.set("mapreduce.jobhistory.address", resourcenode + ":10020");//mapred-site.xml查看指定JobHistory Server（服务器端）的地址
        conf.set("mapreduce.job.jar", ShoppingDataAnalysisJarUtil.jar(MallSalesCount.class));
        return conf;
    }
}

