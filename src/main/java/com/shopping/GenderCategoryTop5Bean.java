package com.shopping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GenderCategoryTop5Bean implements WritableComparable<GenderCategoryTop5Bean> {
    private String gender;
    private String category;
    private double totalAmount;

    public GenderCategoryTop5Bean() {
    }

    public GenderCategoryTop5Bean(String gender, String category, double totalAmount) {
        this.gender = gender;
        this.category = category;
        this.totalAmount = totalAmount;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getGender() {
        return gender;
    }

    public String getCategory() {
        return category;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.gender);
        dataOutput.writeUTF(this.category);
        dataOutput.writeDouble(this.totalAmount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.gender = dataInput.readUTF();
        this.category = dataInput.readUTF();
        this.totalAmount = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return gender + "\t" + category + "\t" + String.format("%.2f", totalAmount);
    }

    @Override
    public int compareTo(GenderCategoryTop5Bean other) {
        // 先按性别排序，再按消费金额降序排序
        int genderCompare = this.gender.compareTo(other.gender);
        if (genderCompare != 0) {
            return genderCompare;
        } else {
            // 相同性别下按消费金额降序排序
            if (other.getTotalAmount() == this.totalAmount) {
                return this.category.compareTo(other.getCategory());
            } else {
                return other.getTotalAmount() > this.totalAmount ? 1 : -1;
            }
        }
    }
}