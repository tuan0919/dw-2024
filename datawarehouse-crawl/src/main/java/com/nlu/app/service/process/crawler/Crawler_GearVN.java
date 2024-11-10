package com.nlu.app.service.process.crawler;

import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;
import java.util.List;

public class Crawler_GearVN implements Crawler {
    @Override
    public String prepareHTML(String source) {
        var driver = prepareDriver(source);
        try {
            Actions actions = new Actions(driver);
            WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(25));
            var gallery = driver.findElement(By.cssSelector(".product-gallery")).getAttribute("outerHTML");
            var infoHeader = driver.findElement(By.cssSelector(".info-header")).getAttribute("outerHTML");
            String tableInfo;
            try {
                tableInfo = driver.findElement(By.cssSelector(".table-technical ul")).getAttribute("outerHTML");
            } catch (NoSuchElementException e) {
                tableInfo = driver.findElement(By.cssSelector("table")).getAttribute("outerHTML");
            }
            // Thay thế tất cả các URL bắt đầu bằng "//" thành URL đầy đủ
            // Lấy URL hiện tại để xác định giao thức
            String baseUrl = driver.getCurrentUrl();
            String protocol = baseUrl.startsWith("https") ? "https:" : "http:";
            gallery = gallery.replaceAll("src=\"//", "src=\"" + protocol + "//");
            gallery = gallery.replaceAll("data-image=\"//", "data-image=\"" + protocol + "//");
            return String.format("""
                    <section id="gearvn_product">
                        %s
                        %s
                        %s
                    </section>
                    """, gallery, infoHeader, tableInfo);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
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
            WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(25));

            // Logic chuẩn bị trích xuất nguồn dữ liệu từ một trang cụ thể
            // Kiểm tra xem button #load_more có tồn tại hay không?
            while (true) {
                try {
                    List<WebElement> loadMoreButtonList = driver.findElements(By.cssSelector("#load_more"));
                    if (!loadMoreButtonList.isEmpty()) {
                        // Kiểm tra xem button có thể bấm được hay không
                        WebElement loadMoreButton = loadMoreButtonList.get(0);
                        if (loadMoreButton.isEnabled() && loadMoreButton.isDisplayed()) {
                            // load thêm dữ liệu
                            actions.moveToElement(loadMoreButton).click().perform();
                            int number = driver
                                    .findElements(By.cssSelector(".collection-product .loaded"))
                                            .size();
                            wait.until(ExpectedConditions.numberOfElementsToBeMoreThan(By.cssSelector(".collection-product .loaded"), number));
                            Thread.sleep(1000);
                            number = driver
                                    .findElements(By.cssSelector(".collection-product .loaded"))
                                    .size();
                            System.out.println("New elements: " + number);
                        }
                    } else {
                        break;
                    }
                    JavascriptExecutor js = (JavascriptExecutor) driver;
                    List<String> hrefList = List.of();
                    var response = js.executeScript("return [...$('.collection-product .loaded .proloop-img>a[href]')].map(e => $(e).prop('href'));");
                    if (response != null) {
                        hrefList = (List<String>) response;
                    }
                    return hrefList;
                } catch (Exception e1) {
                    e1.printStackTrace();
                    break;
                }
            }

        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            driver.quit();
        }
        return List.of();
    }

    public static void main(String[] args) {
        var instance = new Crawler_GearVN();
        System.out.println(instance.prepareHTML("https://gearvn.com/products/chuot-dareu-khong-day-em911x-rgb-trang"));
    }
}