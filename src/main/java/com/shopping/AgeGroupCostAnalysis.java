package com.shopping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class AgeGroupCostAnalysis {

    /**
     * Mapper类：读取数据，根据年龄划分年龄段，输出<年龄段, 单次消费金额>
     */
    public static class AgeGroupMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text ageGroup = new Text();
        private DoubleWritable amount = new DoubleWritable();

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

                if (fields.length < 7) {
                    return; // 数据不完整，跳过
                }

                // 提取字段
                // 字段顺序：invoice_no, customer_id, gender, age, category, quantity, price, payment_method, invoice_date, shopping_mall
                int age = Integer.parseInt(fields[3].trim());
                int quantity = Integer.parseInt(fields[5].trim());
                double price = Double.parseDouble(fields[6].trim());

                // 过滤未成年人（年龄小于18岁）
                if (age < 18) {
                    return;
                }

                // 计算单次消费金额
                double singleAmount = quantity * price;

                // 根据年龄划分年龄段
                String group = getAgeGroup(age);

                ageGroup.set(group);
                amount.set(singleAmount);

                context.write(ageGroup, amount);

            } catch (Exception e) {
                // 数据解析异常，跳过该行
                System.err.println("Error parsing line: " + line);
            }
        }

        /**
         * 根据年龄返回对应的年龄段
         */
        private String getAgeGroup(int age) {
            if (age >= 18 && age <= 25) {
                return "18-25";
            } else if (age >= 26 && age <= 35) {
                return "26-35";
            } else if (age >= 36 && age <= 45) {
                return "36-45";
            } else if (age >= 46 && age <= 55) {
                return "46-55";
            } else { // age >= 56
                return "56+";
            }
        }
    }

    /**
     * Reducer类：计算每个年龄段的平均单次消费金额
     */
    public static class AgeGroupReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalAmount = 0.0;
            int count = 0;

            // 累加所有消费金额
            for (DoubleWritable val : values) {
                totalAmount += val.get();
                count++;
            }

            // 计算平均单次消费金额
            double avgAmount = totalAmount / count;

            // 格式化输出：总消费金额、交易次数、平均单次消费
            String result = String.format("总消费:%.2f TL\t交易次数:%d\t平均消费:%.2f TL",
                    totalAmount, count, avgAmount);

            context.write(key, new Text(result));
        }
    }

    /**
     * Driver类：配置并启动MapReduce任务
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: AgeGroupCostAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Cost Analysis");

        // 设置Jar包
        job.setJarByClass(AgeGroupCostAnalysis.class);

        // 设置Mapper和Reducer
        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);

        // 设置Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

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