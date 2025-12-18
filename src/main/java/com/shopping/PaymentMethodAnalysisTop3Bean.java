package com.shopping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class PaymentMethodAnalysisTop3Bean implements WritableComparable<PaymentMethodAnalysisTop3Bean> {

    private String category;      // 产品类别
    private int count;            // 交易次数

    // 无参构造函数（必须）
    public PaymentMethodAnalysisTop3Bean() {
    }

    // 带参构造函数
    public PaymentMethodAnalysisTop3Bean(String category, int count) {
        this.category = category;
        this.count = count;
    }

    // Getter和Setter方法
    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    /**
     * 序列化方法：将对象写入输出流
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(category);
        out.writeInt(count);
    }

    /**
     * 反序列化方法：从输入流读取对象
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.category = in.readUTF();
        this.count = in.readInt();
    }

    /**
     * 比较方法：按交易次数降序排序
     * 返回负数表示this < o，返回正数表示this > o
     */
    @Override
    public int compareTo(PaymentMethodAnalysisTop3Bean o) {
        // 降序：次数大的排在前面
        return Integer.compare(o.count, this.count);
    }

    @Override
    public String toString() {
        return category + "\t" + count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PaymentMethodAnalysisTop3Bean that = (PaymentMethodAnalysisTop3Bean) o;

        if (count != that.count) return false;
        return category != null ? category.equals(that.category) : that.category == null;
    }

    @Override
    public int hashCode() {
        int result = category != null ? category.hashCode() : 0;
        result = 31 * result + count;
        return result;
    }
}