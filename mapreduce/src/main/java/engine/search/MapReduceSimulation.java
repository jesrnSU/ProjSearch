package engine.search;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class MapReduceSimulation {
    private int numberOfMappers;
    private File file;

    public MapReduceSimulation(int numberOfMappers, File file) {
        this.numberOfMappers = numberOfMappers;
        this.file = file;
    }

    public void runMapAndSort() {
        long fileSize = file.length();
        long chunkSize = fileSize / numberOfMappers;

        List<ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>> partialMaps = new ArrayList<>();
        List<Thread> mapperThreads = new ArrayList<>();
        List<Thread> sorterThreads = new ArrayList<>();
        List<LinkedBlockingDeque<CoursePage>> allDeques = new ArrayList<>();

        for(int i = 0; i < numberOfMappers; i++){
            LinkedBlockingDeque<CoursePage> queue = new LinkedBlockingDeque<>(100);
            allDeques.add(queue);
            
            ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> localMap = new ConcurrentHashMap<>();
            partialMaps.add(localMap);

            long start = i * chunkSize;
            long end = start + chunkSize;

            if(i == (numberOfMappers - 1)){
                end = fileSize;
            }

            FileChunkReader mapper = new FileChunkReader(file, start, end, queue);
            Thread mapThread = new Thread(mapper);
            mapperThreads.add(mapThread);
            mapThread.start();

            for(int j = 0; j < 2; j++) {
                QueueSorter sorter = new QueueSorter(queue, localMap);
                Thread sortThread = new Thread(sorter);
                sorterThreads.add(sortThread);
                sortThread.start();
            }
        }

        try{
            for(Thread t : mapperThreads){
                t.join();
            }
            System.out.println("All mappers have finished reading");

            for(LinkedBlockingDeque<CoursePage> activeDeque : allDeques){
                activeDeque.put(Main.END_PAGE);
                activeDeque.put(Main.END_PAGE);
            }

            for(Thread t : sorterThreads){
                t.join();
            }
            System.out.println("Sorting is done");

            int i = 0;
            for(ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> map : partialMaps){
                System.out.println("--- Result for Mapper " + i + " ---");
                printResults(map);
                i++;
            }

        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }

    private void printResults(ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> map) {
        for (Map.Entry<String, ConcurrentHashMap<String, Integer>> entry : map.entrySet()) {
            System.out.println("WORD : " + entry.getKey());
            Map<String, Integer> innerMap = entry.getValue();
            for (Map.Entry<String, Integer> urlAndCount : innerMap.entrySet()) {
                System.out.println("\tURL: " + urlAndCount.getKey() + " count: " + urlAndCount.getValue());
            }
        }
    }
}