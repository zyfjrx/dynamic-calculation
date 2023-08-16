package test;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class WordCount {
    public static void main(String[] args) throws Exception {
        HashMap<String,String> defaultMap = new HashMap<>();
        defaultMap.put("username","zs");
        defaultMap.put("port","3306");
        Map<String, String> data = Collections.unmodifiableMap(defaultMap);


        HashMap<String,String> exMap = new HashMap<>();
        defaultMap.put("port","33060");
        defaultMap.put("host","hadoop");
        Map<String, String> otherData = Collections.unmodifiableMap(exMap);

        Map<String, String> resultData = new HashMap<>(data.size() + otherData.size());
        resultData.putAll(data);
        resultData.putAll(otherData);

        Set<Object> unP = Collections.newSetFromMap(new ConcurrentHashMap<>(resultData.size()));
        unP.addAll(resultData.keySet());
        System.out.println(resultData);
        System.out.println(unP);

        HashSet<String> left = new HashSet<>(data.keySet());
        left.removeAll(unP);
        System.out.println(left);

        HashSet<String> right = new HashSet<>(otherData.keySet());
        right.removeAll(unP);
        System.out.println(right);

        unP.removeAll(left);
        unP.removeAll(right);
        System.out.println(resultData);

    }
}
