package com.nlu.app.service.process.crawler;

import org.openqa.selenium.WebDriver;

import java.util.List;

public interface Crawler {
    /**
     * Logic crawl ra cấu trúc HTML mà tại đó, có thể extract được đầy đủ các field cần thiết của một sản phẩm.
     * @param source nguồn sản phẩm
     * @return cấu trúc HTML dưới dạng String của một sản phẩm
     */
    String prepareHTML(String source);
    WebDriver prepareDriver(String source);
    /**
     * Logic chuẩn bị trích xuất nguồn dữ liệu từ một trang cụ thể.
     * @param page Trang chứa các nguồn sản phẩm
     * @return một mảng link nguồn dữ liệu cho từng sản phẩm.
     */
    List<String> prepareSources(String page);
}
