package com.nlu.app;
import com.nlu.app.service.CellphoneService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class DatawarehouseCrawlApplication {
    private final CellphoneService cellphoneService;
    public static void main(String[] args) throws Exception {
        SpringApplication.run(DatawarehouseCrawlApplication.class, args);
    }

    @PostConstruct
    public void test() throws Exception {
//        var sources = cellphoneService
//                .getSourcesFromPage("https://cellphones.com.vn/phu-kien/chuot-ban-phim-may-tinh/chuot.html");
//        var pools = Executors.newFixedThreadPool(1);
//        List<CompletableFuture<Void>> futures = new ArrayList<>();
//        for (var source : sources) {
//            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
//                try {
//                    cellphoneService.crawlProduct(source);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }, pools);
//            futures.add(future);
//        }
//        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
//        allOf.get();
//        pools.shutdown();
        var sources = cellphoneService
                .getSourcesFromPage("https://cellphones.com.vn/phu-kien/chuot-ban-phim-may-tinh/chuot.html");
        for (var source: sources) {
            cellphoneService.crawlProduct(source);
        }
    }
}
