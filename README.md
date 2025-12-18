随着电子商务和线下零售的快速发展，购物中心积累了大量的交易数据。本次考查要求利用Hadoop框架和MapReduce编程，对伊斯坦布尔10个购物中心2021-2023年的购物数据进行分析，以了解不同群体的购物习惯和消费模式。
通过MapReduce编程，从多维度分析购物中心购物数据，具体任务包括：

任务一：各购物中心销售表现分析
（1）子任务1：计算每个产品类别的交易次数
实现CategorySalesCount类：统计每个产品类别的交易次数。输出结果至HDFS路径/ShoppingDataAnalysis/CategorySalesCountOutput，分析最受欢迎的产品类别。
（2）子任务2：计算各购物中心的销售额排名
1、实现MallSalesCount类：统计每个购物中心的总销售额（销售额 = quantity×price）。统计每个购物中心的交易总次数（按invoice_no去重计数）。输出结果至 HDFS 路径/ShoppingDataAnalysis/MallSalesCountOutput，并分析不同购物中心的销售表现差异。

任务二：性别与产品类别的消费偏好分析
（1）子任务 1：按性别和产品类别计算消费金额相关指标
实现GenderCategoryPreference类：按性别（gender）和购买产品类别（category）分组，计算每组的消费总金额（总金额 = sum (quantity×price)）和平均单次消费金额。输出结果至 HDFS 路径/ShoppingDataAnalysis/GenderCategoryPreferenceOutput，分析性别、产品类别与消费能力的关系。
（2）子任务 2：筛选男女用户消费金额最高的 Top5 产品类别
实现GenderCategoryTop5类及GenderCategoryTop5Bean类，筛选出男女用户各自消费金额最高的 5 类产品。输出结果至 HDFS 路径/ShoppingDataAnalysis/GenderCategoryTop5Output，在此路径中分开存储。比较性别间的消费偏好差异。

任务三：年龄段与产品类别的消费能力分析
（1）子任务 1：各年龄段平均单次消费金额分析
实现AgeGroupCostAnalysis类：将年龄划分为 5 个区间（18-25 岁、26-35 岁、36-45 岁、46-55 岁、56 岁以上）（未成年不允许网购），计算每个年龄段的平均单次消费金额（单次消费金额 = quantity×price）。输出结果至 HDFS 路径/ShoppingDataAnalysis/AgeGroupCostAnalysisOutput/，分析年龄段与消费能力的关联。

（2）子任务 2：指定产品类别在各年龄段的消费差异分析
实现AgeGroupCategory类，计算指定产品类别(如Category=Clothing)在各年龄段的平均单次消费金额，结果输出至HDFS路径/ShoppingDataAnalysis/AgeGroupCategoryOutput，分析不同年龄段对同一产品类别的消费差异。

任务四：付款方式与消费行为关联分析
（1）子任务 1：付款方式使用次数及占比统计
实现PaymentMethodAnalysis类：统计每种付款方式的使用次数及占比（占比 = 某方式次数 / 总次数）。输出结果至HDFS路径/ShoppingDataAnalysis/PaymentMethodAnalysisOutput/，分析用户支付习惯。
（2）子任务 2：不同付款方式下最常购买的 Top3 产品类别分析
实现PaymentMethodAnalysisTop3类和PaymentMethodAnalysisTop3Bean类：分析不同付款方式下最常购买的前 3 类产品（按交易次数排序）。输出结果至 HDFS 路径/ShoppingDataAnalysis/PaymentMethodAnalysisTop3Output/，总结付款方式与消费类型的关联规律。
