package com.nlu.app.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

@Service
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class DataCrawlService {

    public static void crawl(String webURL, WebDriver driver) throws InterruptedException {
        // Khởi tạo ChromeDriver với ChromeOptions
        driver.get(webURL);
        Actions actions = new Actions(driver);

        // Cuộn trang xuống bằng phím Page Down
        for (int i = 0; i < 5; i++) { // Cuộn xuống 5 lần
            actions.sendKeys(Keys.PAGE_DOWN).perform();
            Thread.sleep(500); // Tạm dừng một chút để trang kịp tải nội dung mới
        }

        WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(25));
        WebElement buttonBlock = wait.until(ExpectedConditions.elementToBeClickable(By.cssSelector(".cps-block-technicalInfo")));
        actions.scrollToElement(buttonBlock);
        var button = driver.findElements(By.cssSelector(".cps-block-technicalInfo .button__show-modal-technical")).get(0);
        actions.moveToElement(button).click().perform();
        var modalContent = driver.findElement(By.cssSelector(".technical-content-modal"));
        // Nếu bạn muốn phân tích thêm các phần tử con trong modal
        List<WebElement> childElements = modalContent.findElements(By.cssSelector(".modal-item-description.mx-2>div")); // Tìm tất cả các phần tử con

        for (WebElement child : childElements) {
            var attribute = child.findElement(By.cssSelector("p")).getText();
            var value = child.findElement(By.cssSelector("div")).getText();
            System.out.printf("%s = %s", attribute, value);
            System.out.println();
        }
        driver.quit();
    }

    public static void main(String[] args) throws InterruptedException {
        DataCrawlService.crawl("https://cellphones.com.vn/chuot-gaming-razer-basilisk-v3.html", new ChromeDriver());
    }
}
