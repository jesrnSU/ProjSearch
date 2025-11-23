package engine.search;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Crawler {
    private Set<String> visitedUrls = new HashSet<>();
    private String outputFile;
    private static final String USER_AGENT = "StudentCourseCrawler/1.0 (Educational Purpose)";

    public Crawler(String outputFile) {
        this.outputFile = outputFile;
    }

    public void crawlSitemap(String sitemapUrl) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            processSitemap(sitemapUrl, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processSitemap(String url, BufferedWriter writer) {
        try {
            Document doc = Jsoup.connect(url)
                    .userAgent(USER_AGENT)
                    .parser(Parser.xmlParser())
                    .get();

            Elements childSitemaps = doc.select("sitemap > loc");
            for (Element sitemap : childSitemaps) {
                processSitemap(sitemap.text(), writer);
            }

            Elements pages = doc.select("url > loc");
            for (Element page : pages) {
                String pageUrl = page.text();
                if (shouldVisit(pageUrl)) {
                    visitPage(pageUrl, writer);
                }
            }
        } catch (IOException e) {
            System.err.println("Error processing sitemap " + url + ": " + e.getMessage());
        }
    }

    private void visitPage(String url, BufferedWriter writer) {
        if (visitedUrls.contains(url)) {
            return;
        }
        visitedUrls.add(url);

        try {
            System.out.println("Visiting: " + url);
            Document doc = Jsoup.connect(url).userAgent(USER_AGENT).get();
            
            String title = doc.title();
            String body = doc.body().text();
            String cleanBody = body.replaceAll("\\s+", " ").trim();

            writer.write(url + "\t" + title + "\t" + cleanBody);
            writer.newLine();

        } catch (IOException e) {
            System.err.println("Error visiting " + url + ": " + e.getMessage());
        } finally {
            try {
                // Dont get banned from SU, make sure this line is always in a finally block!!!
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean shouldVisit(String url) {
        return url.contains("/utbildning/") && !url.endsWith(".pdf");
    }
}
