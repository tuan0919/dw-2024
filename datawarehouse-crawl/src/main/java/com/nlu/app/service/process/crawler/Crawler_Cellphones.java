package com.nlu.app.service.process.crawler;

import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;
import java.util.List;

public class Crawler_Cellphones implements Crawler {
    @Override
    public String prepareHTML(String source) {
        var driver = prepareDriver(source);
        try {
            Actions actions = new Actions(driver);
            WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(25));
            // Cuộn trang xuống bằng phím Page Down
            for (int i = 0; i < 5; i++) { // Cuộn xuống 5 lần
                actions.sendKeys(Keys.PAGE_DOWN).perform();
                Thread.sleep(1000); // Tạm dừng một chút để trang kịp tải nội dung mới
            }
            WebElement buttonBlock = wait.until(ExpectedConditions.elementToBeClickable(By.cssSelector(".cps-block-technicalInfo")));
            actions.scrollToElement(buttonBlock);
            var button = driver.findElements(By.cssSelector(".cps-block-technicalInfo .button__show-modal-technical")).get(0);
            actions.moveToElement(button).click().perform();
            var modalContent = driver.findElement(By.cssSelector(".technical-content-modal")).getAttribute("outerHTML");
            var gallery = driver.findElement(By.cssSelector(".gallery-product-detail.mb-2")).getAttribute("outerHTML");
            var priceBox = driver.findElement(By.cssSelector(".box-info__box-price")).getAttribute("outerHTML");
            var headerBox = driver.findElement(By.cssSelector(".box-header")).getAttribute("outerHTML");
            return String.format("""
                    <section id="cellphone_product">
                        %s
                        %s
                        %s
                        %s
                    </section>
                    """, modalContent, gallery, priceBox, headerBox);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getCause());
        } finally {
            driver.quit();
        }
    }

    @Override
    public WebDriver prepareDriver(String source) {
        // Tạo ChromeOptions và set chế độ chạy ẩn
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--headless"); // Chế độ chạy ẩn
        options.addArguments("--disable-gpu"); // Tắt GPU (tùy chọn này có thể cần trên Windows)
        options.addArguments("--no-sandbox"); // Tùy chọn an toàn hơn cho môi trường Linux
        options.addArguments("--disable-dev-shm-usage"); // Giảm thiểu tài nguyên bộ nhớ chia sẻ

        // Khởi tạo ChromeDriver với ChromeOptions
        WebDriver driver = new ChromeDriver(options);
        driver.get(source);
        return driver;
    }

    @Override
    public List<String> prepareSources(String page) {
        var driver = prepareDriver(page);
        try {
            Actions actions = new Actions(driver);
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
            int size = products.size();
            return products.stream()
                    .map(product -> product.findElement(By.cssSelector("a[href]")))
                    .map(anchor -> anchor.getAttribute("href"))
                    .toList();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getCause());
        } finally {
            driver.quit();
        }
    }
}
