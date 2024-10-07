package com.nlu.app;

import com.nlu.app.service.DataCrawlService;
import com.opencsv.CSVWriter;
import lombok.RequiredArgsConstructor;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

@SpringBootApplication
@RequiredArgsConstructor
public class DatawarehouseCrawlApplication {
    public static void main(String[] args) throws InterruptedException, IOException {
        SpringApplication.run(DatawarehouseCrawlApplication.class, args);

        // Tạo ChromeOptions và set chế độ chạy ẩn
        ChromeOptions options = new ChromeOptions();

        // Khởi tạo ChromeDriver với ChromeOptions
        WebDriver driver = new ChromeDriver(options);
        Actions actions = new Actions(driver);
        driver.get("https://cellphones.com.vn/phu-kien/chuot-ban-phim-may-tinh/chuot.html");

        // Khởi tạo WebDriverWait
        WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(10));
        while (true) {
            try {
                // Kiểm tra xem nút "Show More" có tồn tại không
                List<WebElement> showMoreButtonList = driver.findElements(By.cssSelector(".btn-show-more"));
                if (showMoreButtonList.isEmpty()) {
                    System.out.println("Nút Show More không còn tồn tại.");
                    break; // Thoát vòng lặp khi nút không còn tồn tại
                }

                WebElement showMoreButton = showMoreButtonList.get(0);

                // Kiểm tra xem nút có thể bấm được hay không
                if (!showMoreButton.isEnabled() || !showMoreButton.isDisplayed()) {
//                    System.out.println("Nút Show More không còn khả năng bấm.");
                    break; // Thoát vòng lặp khi nút không còn khả năng bấm
                }

                // Chờ cho nút ".btn-show-more" không có class "is-loading" và "is-large"
                wait.until(ExpectedConditions.not(ExpectedConditions.attributeContains(showMoreButton, "class", "is-loading")));
                wait.until(ExpectedConditions.not(ExpectedConditions.attributeContains(showMoreButton, "class", "is-large")));

                // Bấm vào nút để load thêm dữ liệu
                actions.moveToElement(showMoreButton).click().perform();

                Thread.sleep(1000); // Tạm dừng để đảm bảo dữ liệu mới được tải

                // Chờ dữ liệu mới xuất hiện
                wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector(".product-info")));
            } catch (Exception e) {
                e.printStackTrace();
                // Thoát vòng lặp nếu có bất kỳ lỗi gì khác
                break;
            }
        }
        // Tìm tất cả các phần tử có class là 'product-info'
        List<WebElement> products = driver.findElements(By.cssSelector(".product-info"));
        CSVWriter writer = new CSVWriter(new FileWriter(System.getProperty("user.home")+"/Desktop/crawl.csv"));
        String[] header = {"nameHtml", "imgHtml", "infoHtml"};
        writer.writeNext(header);
        String nameHtml;
        String imgHtml;
        String infoHtml;
        int count = 0;
        for (WebElement product : products) {
            try {
                    var href = product.findElement(By.cssSelector("a[href]"));
                    var name = product.findElement(By.cssSelector(".product__name"));
                    nameHtml = name.getAttribute("outerHTML");
                    var imageLink = product.findElement(By.cssSelector(".product__image img"));
                    imgHtml = imageLink.getAttribute("outerHTML");
                    String productLink = href.getAttribute("href");
                    System.out.println("product link: "+productLink);
                    infoHtml = DataCrawlService.crawl(productLink);
                    System.out.println("-------------------------------------------");
                    writer.writeNext(new String[]{nameHtml, imgHtml, infoHtml});
                    count++;
            } catch (Exception e) {
                e.printStackTrace(); // skip product bị lỗi
            }
        }
        System.out.println("Tổng số sản phẩm crawl được: "+count);
        driver.quit();
    }

}
