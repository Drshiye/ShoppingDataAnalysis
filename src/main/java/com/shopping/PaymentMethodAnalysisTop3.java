package com.shopping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;


public class PaymentMethodAnalysisTop3 {

    /**
     * Mapper类：提取付款方式和产品类别的组合
     */
    public static class PaymentCategoryMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text paymentMethod = new Text();
        private Text category = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // 跳过表头
            if (line.startsWith("invoice_no")) {
                return;
            }

            try {
                // 分割CSV行
                String[] fields = line.split(",");

                if (fields.length < 8) {
                    return;
                }

                // 提取产品类别和付款方式
                String cat = fields[4].trim();           // category
                String payment = fields[7].trim();       // payment_method

                paymentMethod.set(payment);
                category.set(cat);

                // 输出：<付款方式, 产品类别>
                context.write(paymentMethod, category);

            } catch (Exception e) {
                System.err.println("Error parsing line: " + line);
            }
        }
    }

    /**
     * Reducer类：统计每种付款方式下各类别的交易次数，并筛选Top3
     */
    public static class PaymentCategoryReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // 使用HashMap统计每个产品类别的交易次数
            Map<String, Integer> categoryCount = new HashMap<>();

            for (Text val : values) {
                String category = val.toString();
                categoryCount.put(category, categoryCount.getOrDefault(category, 0) + 1);
            }

            // 将结果转换为Bean列表
            List<PaymentMethodAnalysisTop3Bean> categoryList = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : categoryCount.entrySet()) {
                categoryList.add(new PaymentMethodAnalysisTop3Bean(entry.getKey(), entry.getValue()));
            }

            // 按交易次数降序排序
            Collections.sort(categoryList);

            // 取前3个
            int topN = Math.min(3, categoryList.size());
            StringBuilder result = new StringBuilder();

            for (int i = 0; i < topN; i++) {
                PaymentMethodAnalysisTop3Bean bean = categoryList.get(i);
                result.append(String.format("Top%d: %s (交易次数:%d)",
                        i + 1, bean.getCategory(), bean.getCount()));
                if (i < topN - 1) {
                    result.append(" | ");
                }
            }

            context.write(key, new Text(result.toString()));
        }
    }

    /**
     * Driver类：配置并启动MapReduce任务
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: PaymentMethodAnalysisTop3 <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Payment Method Top3 Category Analysis");

        // 设置Jar包
        job.setJarByClass(PaymentMethodAnalysisTop3.class);

        // 设置Mapper和Reducer
        job.setMapperClass(PaymentCategoryMapper.class);
        job.setReducerClass(PaymentCategoryReducer.class);

        // 设置Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}