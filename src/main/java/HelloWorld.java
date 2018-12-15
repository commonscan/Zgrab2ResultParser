import com.github.openjson.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.ToHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class HelloWorld {
    public static void parser(String source) {
        LinkContentHandler linkHandler = new LinkContentHandler();
        ContentHandler textHandler = new BodyContentHandler();
        ToHTMLContentHandler toHTMLHandler = new ToHTMLContentHandler();
        TeeContentHandler teeHandler = new TeeContentHandler(linkHandler, textHandler, toHTMLHandler);
        Metadata metadata = new Metadata();
        ParseContext parseContext = new ParseContext();
        HtmlParser parser = new HtmlParser();
        InputStream in = null;
        try {
            in = org.apache.commons.io.IOUtils.toInputStream(source, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            parser.parse(in, teeHandler, metadata, parseContext);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TikaException e) {
            e.printStackTrace();
        }

        System.out.println("title:  " + metadata.get("title"));
//        System.out.println("links:\n" + linkHandler.getLinks());
//        System.out.println("text:\n" + textHandler.toString());
//        System.out.println("html:\n" + toHTMLHandler.toString());
    }

    public static void parseLine(String line) {
        JSONObject obj = new JSONObject(line);
        String body = obj.getJSONObject("data").getJSONObject("http").getJSONObject("result").getJSONObject("response").getString("body");
        HelloWorld.parser(body);
    }

    public static void loadFile(String path) {
        try (Stream<String> stream = Files.lines(Paths.get(path))) {
//            stream.forEach(System.out::println);
            stream.forEach(HelloWorld::parseLine);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main1(String file_path) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java1WordCount")
                .config("spark.master", "local")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(file_path).javaRDD();
        System.out.println();
        JavaRDD<String> words = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                HelloWorld.parseLine(s);
                return null;
            }
        });
        words.collect();
        spark.stop();
    }

    public static void main(String[] args) throws Exception {
        main1("./example.json");
    }

}
