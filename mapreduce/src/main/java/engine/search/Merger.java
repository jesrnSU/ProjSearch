package engine.search;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Merger {
    private List<ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>> partialMaps;

    public Merger(List<ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>> partialMaps) {
        this.partialMaps = partialMaps;
    }

    public HashMap<String, HashMap<String, Integer>> mergeAll() {
        HashMap<String, HashMap<String, Integer>> masterMap = new HashMap<>();

        for (ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> partialMap : partialMaps) {
            
            for (Map.Entry<String, ConcurrentHashMap<String, Integer>> wordEntry : partialMap.entrySet()) {
                String word = wordEntry.getKey();
                ConcurrentHashMap<String, Integer> urlCounts = wordEntry.getValue();

                HashMap<String, Integer> masterUrlCounts = masterMap.get(word);
                if (masterUrlCounts == null) {
                    masterUrlCounts = new HashMap<>();
                    masterMap.put(word, masterUrlCounts);
                }

                for (Map.Entry<String, Integer> urlEntry : urlCounts.entrySet()) {
                    masterUrlCounts.merge(urlEntry.getKey(), urlEntry.getValue(), Integer::sum);
                }
            }
        }

        return masterMap;
    }
}