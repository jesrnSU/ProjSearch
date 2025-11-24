package engine.search;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class QueueSorter implements Runnable {
    private LinkedBlockingDeque<CoursePage> queue;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> wordCollection;

    public QueueSorter(LinkedBlockingDeque<CoursePage> queue, ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> wordCollection) {
        this.queue = queue;
        this.wordCollection = wordCollection;
    }

    @Override
    public void run() {
        CoursePage page;
        while (true) {
            try {
                page = queue.take();

                if (page == Main.END_PAGE) {
                    break;
                }

                String url = page.getWebUrl();
                String[] words = page.getContent().toLowerCase().split("\\s+");

                invertedIndexForWord(url, words);
           } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void invertedIndexForWord(String url, String[] words){
        for (String word : words) {
            if (word.isEmpty())
                continue;

            String strippedWord = word.replaceAll("[^\\p{L}\\p{Z}]", "");
                if (strippedWord.isEmpty())
                    continue;

                wordCollection.putIfAbsent(strippedWord, new ConcurrentHashMap<>());
                ConcurrentHashMap<String, Integer> innerMap = wordCollection.get(strippedWord);
                innerMap.merge(url, 1, Integer::sum);
        }
    }
}
