package edu.ruc.utils;

import java.io.*;

/**
 * Created by Tao on 2017/4/15.
 */
public class FileUtil {
    /**
     * @param file
     * @return String
     * @Title: readFile
     * @Description: 文件的读和写
     */
    public static String readFile(File file) {
        FileInputStream fileInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        try {
            // 获取磁盘的文件
            // File file = new File(fileName);
            // 开始读取磁盘的文件
            fileInputStream = new FileInputStream(file);
            // 创建一个字节流
            inputStreamReader = new InputStreamReader(fileInputStream);
            // 创建一个字节的缓冲流
            bufferedReader = new BufferedReader(inputStreamReader);
            StringBuffer buffer = new StringBuffer();
            String string = null;
            while ((string = bufferedReader.readLine()) != null) {
                buffer.append(string + "\n");
            }
            return buffer.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                bufferedReader.close();
                inputStreamReader.close();
                fileInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param fileName
     * @return String
     * @Title: readFile
     * @Description:方法的重载
     */
    public static String readFile(String fileName) {
        return readFile(new File(fileName));

    }

    /**
     * @Description: write
     * @param content
     * @param filename
     */
    public static void writeFile(String content, String filename) {
        // 要写入的文件
        File file = new File(filename);
        // 写入流对象
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(file);
            printWriter.print(content);
            printWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (printWriter != null) {
                try {
                    printWriter.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
        }
    }

    /**
     * @param args void
     * @Title: main
     * @Description: 入口函数
     */
    public static void main(String[] args) {
        // 公式：内容+模板=文件
        String pack = "com.hh.server";
        String model = "server";
        String rootPath = "E:/JSP Project/minjieshi/";
        String srcPath = rootPath + "src/template/entity.txt";
        System.out.println("1. " + srcPath);
        // 获取模板的内容
        String templateContent = readFile(srcPath);
        templateContent = templateContent.replaceAll("\\[package\\]", pack).replaceAll("\\[model\\]", model);
        // 将替换的内容写入到工程的目录下面
        String path = pack.replaceAll("\\.", "/");
        System.out.println("2. " + path);
        String filePath = rootPath + "src/" + path;
        System.out.println("3. " + filePath);
        File rootFile = new File(filePath);
        if (!rootFile.exists()) {
            rootFile.mkdirs();
        }
        String fileName = filePath + "/" + model + ".java";
        System.out.println("4. " + fileName);
        writeFile(templateContent, fileName);
    }
}
