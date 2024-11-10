package com.nlu.app.service.process.extractor;

import com.nlu.app.pojo.CrawlFields;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.util.ArrayList;
import java.util.List;

public class Extractor_Gearvn implements Extractor {
    @Override
    public String[] extract(String html, List<CrawlFields> fields) {
        int col = fields.size();
        var result = new String[fields.size()];
        Document doc = Jsoup.parse(html);
        for (int i = 0; i < col; i++) {
            var field = fields.get(i);
            var elements = this.extractElements(doc, field);
            int size = elements.size();
            String data;
            if (size == 0) {
                data = "";
                System.out.println("field: "+ field.getField_name() +" bị thiếu data");
                result[i] = data;
            }
            else if (size == 1) {
                data = this.extractSingleData(elements.get(0), field);
                System.out.println("field: "+ field.getField_name() +", data: "+data);
                result[i] = data;
            }
            else {
                ArrayList<String> arr = new ArrayList<>();
                for (var element: elements) arr.add(this.extractSingleData(element, field));
                data = String.join(", ", arr);
                System.out.println("field: "+ field.getField_name() +", data: "+data);
                result[i] = data;
            }
        }
        return result;
    }
}
