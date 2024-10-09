package hiro;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CSVHtmlExtractor1 {

    public static void main(String[] args) throws CsvValidationException {
        String inputCsv = "D:\\Downloads\\crawl.csv"; // Đường dẫn đến tệp CSV đầu vào
        String outputCsv = "D:\\Downloads\\crawl1.csv"; // Đường dẫn đến tệp CSV đầu ra

        try (CSVReader csvReader = new CSVReader(new FileReader(inputCsv));
             CSVWriter csvWriter = new CSVWriter(new FileWriter(outputCsv))) {

            String[] nextLine;

            // Ghi tiêu đề vào file CSV mới
            String[] header = { "Tên sản phẩm", "Hình ảnh", "Kích thước", "Trọng lượng", "Độ phân giải", "Cảm biến",
                    "Nút nhấn", "Kết nối", "Pin", "Tương thích", "Tiện ích", "Hãng sản xuất" };
            csvWriter.writeNext(header);

            // Đọc từng dòng của tệp CSV đầu vào
            while ((nextLine = csvReader.readNext()) != null) {
                // Cột chứa HTML nằm ở vị trí cuối (vị trí thứ 3, chỉ số 2)
                String htmlContentName = nextLine[0];
                String htmlContentImg = nextLine[1];
                String htmlContent = nextLine[2];

                // Trích xuất tên sản phẩm từ cột đầu tiên
                Document doc1s = Jsoup.parse(htmlContentName);
                String productName = doc1s.select("div.product__name h3").text();

                // Trích xuất đường dẫn hình ảnh từ cột thứ hai
                Document doc2 = Jsoup.parse(htmlContentImg);
                String img = doc2.select("img.product__img").attr("src");

                // Phân tích cú pháp HTML trong cột thứ ba
                Document doc = Jsoup.parse(htmlContent);

                // Trích xuất dữ liệu từ HTML
                String size = extractAttribute(doc, "Kích thước");
                String weight = extractAttribute(doc, "Trọng lượng");
                String resolution = extractAttribute(doc, "Độ phân giải");
                String sensor = extractAttribute(doc, "Cảm biến");
                String buttons = extractAttribute(doc, "Nút nhấn");
                String connection = extractAttribute(doc, "Kết nối");
                String battery = extractAttribute(doc, "Pin");
                String compatibility = extractAttribute(doc, "Tương thích");
                String utility = extractAttribute(doc, "Tiện ích");
                String manufacturer = extractAttribute(doc, "Hãng sản xuất");

                // Ghi dữ liệu vào tệp CSV đầu ra
                String[] extractedData = { productName, img, size, weight, resolution, sensor, buttons, connection,
                        battery, compatibility, utility, manufacturer };
                csvWriter.writeNext(extractedData);
            }

            System.out.println("Dữ liệu đã được trích xuất và ghi vào " + outputCsv);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Phương thức để trích xuất thông tin dựa trên tên thuộc tính
    private static String extractAttribute(Document doc, String attributeName) {
        for (Element item : doc.select("div.modal-item-description")) {
            // Lặp qua tất cả các cặp <p> và <div> chứa thông tin
            for (Element row : item.select("div.is-flex")) {
                String label = row.select("p").text(); // Nhãn (vd: Kích thước)
                String value = row.select("div").last().text(); // Chỉ lấy giá trị cuối cùng từ div

                // Nếu nhãn chứa tên thuộc tính mong muốn, trả về giá trị
                if (label.contains(attributeName)) {
                    return value;
                }
            }
        }
        return "None"; // Nếu không tìm thấy thuộc tính nào phù hợp, trả về "None"
    }
}
