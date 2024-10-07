package com.nlu.app.service;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;

public class FileService {
    public static void main(String[] args) throws IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter(System.getProperty("user.home")+"/Desktop/crawl.csv"))) {
            String[] header = {"Column1", "Column2"};
            writer.writeNext(header);
            String[] record1 = {"Value1", "Value2"};
            writer.writeNext(record1);
            String[] record2 = {"Value3", "Value4"};
            writer.writeNext(record2);
            // Thêm nhiều bản ghi hơn nếu cần
        }
    }
}
