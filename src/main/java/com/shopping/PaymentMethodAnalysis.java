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
import java.util.HashMap;
import java.util.Map;


public class PaymentMethodAnalysis {

    /**
     * Mapper类：提取付款方式
     */
    public static class PaymentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text paymentMethod = new Text();
        private final static IntWritable one = new IntWritable(1);

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

                // 提取付款方式字段（第7列，索引为7）
                // 字段顺序：invoice_no, customer_id, gender, age, category, quantity, price, payment_method, invoice_date, shopping_mall
                String payment = fields[7].trim();

                paymentMethod.set(payment);
                context.write(paymentMethod, one);

            } catch (Exception e) {
                System.err.println("Error parsing line: " + line);
            }
        }
    }

    /**
     * Reducer类：统计次数并计算占比
     * 需要两次遍历：第一次计算总数，第二次计算占比
     */
    public static class PaymentReducer extends Reducer<Text, IntWritable, Text, Text> {

        private Map<String, Integer> paymentCounts = new HashMap<>();
        private int totalCount = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;

            // 统计该付款方式的使用次数
            for (IntWritable val : values) {
                count += val.get();
            }

            // 保存到HashMap中
            paymentCounts.put(key.toString(), count);
            totalCount += count;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 在cleanup阶段输出结果，此时已知总次数
            for (Map.Entry<String, Integer> entry : paymentCounts.entrySet()) {
                String payment = entry.getKey();
                int count = entry.getValue();
                double percentage = (count * 100.0) / totalCount;

                // 格式化输出：使用次数、占比
                String result = String.format("使用次数:%d\t占比:%.2f%%\t总交易数:%d",
                        count, percentage, totalCount);

                context.write(new Text(payment), new Text(result));
            }
        }
    }

    /**
     * Driver类：配置并启动MapReduce任务
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: PaymentMethodAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Payment Method Analysis");

        // 设置Jar包
        job.setJarByClass(PaymentMethodAnalysis.class);

        // 设置Mapper和Reducer
        job.setMapperClass(PaymentMapper.class);
        job.setReducerClass(PaymentReducer.class);

        // 设置Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 只使用一个Reducer，确保能计算总占比
        job.setNumReduceTasks(1);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}