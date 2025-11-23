package engine.search;

import java.io.File;

public class Main {

    public static final CoursePage END_PAGE = new CoursePage("ENDING#", "");
    private static final String SITE_MAP = "";
    private static final String OUTPUT_FILE = "crawled_data.txt";

    public static void main(String[] args) {
        // Default number of mappers
        int numberOfMappers = 1;
        if(args.length != 0)
            numberOfMappers = Integer.parseInt(args[0]);

        //Crawler crawler = new Crawler(OUTPUT_FILE);
        // Update to return a file instead? 
        //crawler.crawlSitemap(SITE_MAP);
        
        System.out.println("Crawling completed. Data saved to " + OUTPUT_FILE);
        File file = new File(OUTPUT_FILE);

        if(file.exists()){
            MapReduceSimulation simulation = new MapReduceSimulation(numberOfMappers, file);
            simulation.runMapAndSort();
        } else{
            System.out.println("File does not exist");
        }
    }
}