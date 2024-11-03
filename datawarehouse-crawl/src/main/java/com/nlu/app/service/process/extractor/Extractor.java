package com.nlu.app.service.process.extractor;

import com.nlu.app.pojo.CrawlFields;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface Extractor {
    /**
     * Extract dữ liệu HTML ra thành một model sản phẩm nào đó
     * @param html mã HTML chứa toàn bộ thông tin của sản phẩm
     * @return entity tương ứng trích xuất ra được
     */
    String[] extract(String html, List<CrawlFields> fields);

    default Elements extractElements(Document doc, CrawlFields field) {
        String selector = field.getSelector();
        switch (field.getType()) {
            case "xpath" -> {
                return doc.selectXpath(selector);
            }
            case "css" -> {
                return doc.select(selector);
            }
        }
        return null;
    }

    default String extractSingleData(Element element, CrawlFields field) {
        String src = field.getAttr();
        if (src.equals("text")) {
            return element.text();
        }
        return element.attr(src);
    }
}
