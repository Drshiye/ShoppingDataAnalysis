package com.shopping;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public class ShoppingDataAnalysisJarUtil {

    // 根据传入的类名生成对应的 JAR 文件。获取类所在的根目录，并将该目录打包成 JAR 文件。
    public static String jar(Class<?> cls) {
        String outputJar = cls.getName() + ".jar"; // 生成 JAR 文件名
        String input = cls.getClassLoader().getResource("").getFile();
        // 获取类的类加载器资源路径
        input = input.substring(0, input.length() - 1);// 去掉路径末尾的 "/"
        input = input.substring(0, input.lastIndexOf("/") + 1);// 获取类所在包的根目录
        jar(input, outputJar);// 调用 jar 方法打包
        return outputJar;// 返回生成的 JAR 文件名
    }

    //初始化 JAR 输出流，并调用递归方法将目录打包成 JAR 文件。
    private static void jar(String inputFileName, String outputFileName) {
        JarOutputStream out = null;// 定义 JarOutputStream
        try {
            out = new JarOutputStream(new FileOutputStream(outputFileName));// 创建 JAR 输出流
            File f = new File(inputFileName);// 创建输入文件对象
            jar(out, f, "");// 调用递归方法打包
        } catch (Exception e) {
            e.printStackTrace();// 打印异常信息
        } finally {
            try {
                out.close();// 关闭 JAR 输出流
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //递归地将目录中的文件打包到 JAR 文件中。
    //如果是目录，递归处理子文件和子目录。
    //如果是文件，将文件内容写入 JAR 文件。
    private static void jar(JarOutputStream out, File f, String base) throws Exception {
        if (f.isDirectory()) {// 如果是目录
            File[] fl = f.listFiles();// 获取目录下的所有文件
            // 注意，这里用左斜杠
            base = base.length() == 0 ? "" : base + "/"; // 设置 JAR 文件中的路径前缀
            for (int i = 0; i < fl.length; i++) {
                jar(out, fl[i], base + fl[i].getName());// 递归处理子文件或子目录
            }
        } else {// 如果是文件
            out.putNextEntry(new JarEntry(base));// 创建 JAR 条目
            FileInputStream in = new FileInputStream(f);// 创建文件输入流
            byte[] buffer = new byte[1024];// 定义缓冲区
            int n = in.read(buffer);// 读取文件内容
            while (n != -1) {
                out.write(buffer, 0, n);// 将文件内容写入 JAR 文件
                n = in.read(buffer);// 继续读取
            }
            in.close();// 关闭文件输入流
        }
    }
}
