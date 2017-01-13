package com.juanpi.bi.mr_example;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

/**
 * Created by gongzi on 2016/1/12.
 */
public class JavaReadFile {

    private static void readUsingFileReader(String fileName) throws IOException {
        File file = new File(fileName);
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();
        fr.close();
    }

    private static void readUsingBufferedReader(String fileName, Charset cs) throws IOException {
        File file = new File(fileName);
        FileInputStream fis = new FileInputStream(file);
        InputStreamReader isr = new InputStreamReader(fis, cs);
        BufferedReader br = new BufferedReader(isr);
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();
    }

    private static void readUsingBufferedReaderJava7(String fileName, Charset cs) throws IOException {
        Path path = Paths.get(fileName);
        BufferedReader br = Files.newBufferedReader(path, cs);
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();
    }

    private static void readUsingBufferedReader(String fileName) throws IOException {
        File file = new File(fileName);
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();
        fr.close();
    }

    private static void readUsingScanner(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        Scanner scanner = new Scanner(path);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            System.out.println(line);
        }
    }

    /**
     * read  file to String list
     * @param fileName
     * @throws IOException
     */
    private static void readUsingFiles(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        //read  file to byte array
        byte[] bytes = Files.readAllBytes(path);
        List allLines = Files.readAllLines(path, StandardCharsets.UTF_8);
    }

    public static void main(String[] args) throws IOException {
        String fileName = "E:\\gongzi_test.txt";
        //using Java 7 Files class to process small files, get complete file data
        System.out.println("-------- readUsingFiles ----------");
        readUsingFiles(fileName);
        //using Scanner class for large files, to read  line by line
        System.out.println("-------- readUsingScanner ----------");
        readUsingScanner(fileName);
        //read  using BufferedReader , to read  line by line
        System.out.println("-------- readUsingBufferedReader ----------");
        readUsingBufferedReader(fileName);

        System.out.println("-------- readUsingBufferedReaderJava7 ----------");
        readUsingBufferedReaderJava7(fileName, StandardCharsets.UTF_8);

        System.out.println("-------- readUsingBufferedReader ----------");
        readUsingBufferedReader(fileName, StandardCharsets.UTF_8);
        //read  using FileReader , no encoding support, not efficient

        System.out.println("-------- readUsingFileReader ----------");
        readUsingFileReader(fileName);
    }
}


