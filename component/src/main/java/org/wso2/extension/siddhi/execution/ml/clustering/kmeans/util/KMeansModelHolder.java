package org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util;


import java.util.HashMap;
import java.util.Map;

public class KMeansModelHolder {

    private static final KMeansModelHolder instance = new KMeansModelHolder();

    private Map<String, KMeansModel> KMeansModelMap = new HashMap<>();

    private KMeansModelHolder(){
    }

    public static KMeansModelHolder getInstance() {
        return instance;
    }

    public Map<String, KMeansModel> getKMeansModelMap() {
        return KMeansModelMap;
    }

    public void setKMeansModelMap(Map<String, KMeansModel> KMeansModelMap) {
        this.KMeansModelMap = KMeansModelMap;
    }

    public KMeansModel getKMeansModel(String name) {
        return KMeansModelMap.get(name);
    }

    public void addKMeansModel(String name, KMeansModel model) {
        KMeansModelMap.put(name, model);
    }

    public Map<String, KMeansModel> getClonedKMeansModelMap() {
        Map<String, KMeansModel> clonedMap = new HashMap<>();
        for (Map.Entry<String, KMeansModel> entry: KMeansModelMap.entrySet()) {
            clonedMap.put(entry.getKey(), new KMeansModel(entry.getValue().getCentroidList()));
        }

        return clonedMap;
    }
}
