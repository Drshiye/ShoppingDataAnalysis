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


public class AgeGroupCategory {

    // 指定要分析的产品类别（可通过配置传入）
    private static final String TARGET_CATEGORY = "Clothing";

    /**
     * Mapper类：筛选指定产品类别，按年龄段输出消费金额
     */
    public static class CategoryAgeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text ageGroup = new Text();
        private DoubleWritable amount = new DoubleWritable();
        private String targetCategory;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 从配置中读取目标类别，如果没有配置则使用默认值
            Configuration conf = context.getConfiguration();
            targetCategory = conf.get("target.category", TARGET_CATEGORY);
            System.out.println("Analyzing category: " + targetCategory);
        }

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
                    return;
                }

                // 提取字段
                int age = Integer.parseInt(fields[3].trim());
                String category = fields[4].trim();
                int quantity = Integer.parseInt(fields[5].trim());
                double price = Double.parseDouble(fields[6].trim());

                // 只处理指定的产品类别
                if (!category.equalsIgnoreCase(targetCategory)) {
                    return;
                }

                // 过滤未成年人
                if (age < 18) {
                    return;
                }

                // 计算单次消费金额
                double singleAmount = quantity * price;

                // 划分年龄段
                String group = getAgeGroup(age);

                ageGroup.set(group);
                amount.set(singleAmount);

                context.write(ageGroup, amount);

            } catch (Exception e) {
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
            } else {
                return "56+";
            }
        }
    }

    /**
     * Reducer类：计算指定产品类别在各年龄段的平均消费
     */
    public static class CategoryAgeReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        private String targetCategory;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            targetCategory = conf.get("target.category", TARGET_CATEGORY);
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalAmount = 0.0;
            int count = 0;

            // 累加该年龄段在指定类别的所有消费
            for (DoubleWritable val : values) {
                totalAmount += val.get();
                count++;
            }

            // 计算平均单次消费金额
            double avgAmount = totalAmount / count;

            // 格式化输出
            String result = String.format("类别:%s\t总消费:%.2f TL\t交易次数:%d\t平均消费:%.2f TL",
                    targetCategory, totalAmount, count, avgAmount);

            context.write(key, new Text(result));
        }
    }

    /**
     * Driver类：配置并启动MapReduce任务
     */
    public static void main(String[] args) throws Exception {

        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: AgeGroupCategory <input path> <output path> [category]");
            System.err.println("Example: AgeGroupCategory /input /output Clothing");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // 如果命令行提供了第三个参数，使用该参数作为目标类别
        if (args.length == 3) {
            conf.set("target.category", args[2]);
            System.out.println("Target category set to: " + args[2]);
        } else {
            conf.set("target.category", TARGET_CATEGORY);
            System.out.println("Using default category: " + TARGET_CATEGORY);
        }

        Job job = Job.getInstance(conf, "Age Group Category Analysis");

        // 设置Jar包
        job.setJarByClass(AgeGroupCategory.class);

        // 设置Mapper和Reducer
        job.setMapperClass(CategoryAgeMapper.class);
        job.setReducerClass(CategoryAgeReducer.class);

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
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}