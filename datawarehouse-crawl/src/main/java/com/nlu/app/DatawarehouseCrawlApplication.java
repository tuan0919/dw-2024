package com.nlu.app;

import lombok.RequiredArgsConstructor;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.List;

@SpringBootApplication
@RequiredArgsConstructor
public class DatawarehouseCrawlApplication {
    public static void main(String[] args) {
        SpringApplication.run(DatawarehouseCrawlApplication.class, args);

        // Tạo ChromeOptions và set chế độ chạy ẩn
        ChromeOptions options = new ChromeOptions();

        // Khởi tạo ChromeDriver với ChromeOptions
        WebDriver driver = new ChromeDriver(options);
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
                    System.out.println("Nút Show More không còn khả năng bấm.");
                    break; // Thoát vòng lặp khi nút không còn khả năng bấm
                }

                // Chờ cho nút ".btn-show-more" không có class "is-loading" và "is-large"
                wait.until(ExpectedConditions.not(ExpectedConditions.attributeContains(showMoreButton, "class", "is-loading")));
                wait.until(ExpectedConditions.not(ExpectedConditions.attributeContains(showMoreButton, "class", "is-large")));

                // Bấm vào nút để load thêm dữ liệu
                showMoreButton.click();

                Thread.sleep(1000); // Tạm dừng để đảm bảo dữ liệu mới được tải

                // Chờ dữ liệu mới xuất hiện
                wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector(".product-info")));

                System.out.println("Đã tải thêm sản phẩm.");
            } catch (Exception e) {
                e.printStackTrace();
                // Thoát vòng lặp nếu có bất kỳ lỗi gì khác
                break;
            }
        }

        // Tìm tất cả các phần tử có class là 'product-info'
        List<WebElement> elements = driver.findElements(By.cssSelector(".product-info a[href]"));

        // In ra src của các phần tử tìm được
        for (WebElement product : elements) {
            String productLink = product.getAttribute("href");
            System.out.println(productLink);
        }
        System.out.println("Tổng số sản phẩm load được: "+elements.size());
        // Đóng trình duyệt sau khi hoàn thành công việc
        driver.quit();
    }

}
