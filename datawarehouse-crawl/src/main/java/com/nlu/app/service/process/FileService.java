package com.nlu.app.service.process;

import com.opencsv.CSVWriter;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@Service
public class FileService {
    public int writeCSV(String[] header, List<String[]> dataRows, String filePath) throws IOException {
        int write = 0;
        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
            // Viết header
            writer.writeNext(header);
            // Viết data
            for (String[] row : dataRows) {
                writer.writeNext(row);
                write++;
            }
        }
        return write;
    }
}
