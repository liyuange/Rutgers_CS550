import java.io.*;
import java.util.*;

public class question_2 {

    final private static String PATH = "/Users/liyuange/GithubWorkspace/Rutgers_CS550/Assignment1_Question2/browsing.txt";
    final private static String OUTPUT1 = "/Users/liyuange/Desktop/output1.txt";
    final private static String OUTPUT2 = "/Users/liyuange/Desktop/output2.txt";
    final private static int THRESHOLD = 100;


    private static HashMap<String, Integer> candidateItems = new HashMap<>();
    private static HashMap<String, Integer> frequentItems = new HashMap<>();
    private static HashMap<HashSet<String>, Integer> candidatePairs = new HashMap<>();
    private static HashMap<HashSet<String>, Integer> frequentPairs = new HashMap<>();
    private static HashMap<HashSet<String>, Integer> candidateTriples = new HashMap<>();
    private static HashMap<HashSet<String>, Integer> frequentTriples = new HashMap<>();

    private static HashMap<String, Double> pairConfidence = new HashMap<>();
    private static HashMap<String, Double> tripleConfidence = new HashMap<>();

    private static void firstPass(File file) throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String line;

        while ((line = bufferedReader.readLine()) != null) {
            String[] items = line.split(" ");

            for (String item : items) {
                if (candidateItems.containsKey(item)) {
                    candidateItems.put(item, candidateItems.get(item) + 1);
                } else {
                    candidateItems.put(item, 1);
                }
            }
        }

        for(String key : candidateItems.keySet()){
            if(candidateItems.get(key) >= THRESHOLD){
                frequentItems.put(key, candidateItems.get(key));
            }
        }
    }

    private static void secondPass(File file) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String line;

        while ((line = bufferedReader.readLine()) != null) {
            String[] items = line.split(" ");

            for(int i = 0; i < items.length; i++){
                if(frequentItems.containsKey(items[i])){
                    for(int j = i + 1; j < items.length; j++){
                        if(frequentItems.containsKey(items[j])){
                            HashSet<String> pair = new HashSet<>();
                            pair.add(items[i]);
                            pair.add(items[j]);

                            if(candidatePairs.containsKey(pair)){
                                candidatePairs.put(pair, candidatePairs.get(pair) + 1);
                            }
                            else {
                                candidatePairs.put(pair, 1);
                            }
                        }
                    }
                }
            }
        }

        for(HashSet<String> key : candidatePairs.keySet()){
            if(candidatePairs.get(key) >= THRESHOLD){
                frequentPairs.put(key, candidatePairs.get(key));
            }
        }
    }

    private static void thirdPass(File file) throws IOException {
        for(HashSet<String> triple : frequentPairs.keySet()){
            for(String item : frequentItems.keySet()){
                if(!triple.contains(item)){
                    HashSet<String> possibleTriple = new HashSet<>(triple);
                    possibleTriple.add(item);
                    candidateTriples.put(possibleTriple, 0);
                }
            }
        }
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String line;

        while ((line = bufferedReader.readLine()) != null) {
            String[] items = line.split(" ");
            List<HashSet<String>> lineTriples = new ArrayList<>();;
                for(int i = 0; i < items.length; i++){
                    for(int j = i + 1; j < items.length; j++){
                        for(int k = j + 1; k < items.length; k++){
                            HashSet<String> possibleTriple = new HashSet<>();
                            possibleTriple.add(items[i]);
                            possibleTriple.add(items[j]);
                            possibleTriple.add(items[k]);
                            lineTriples.add(possibleTriple);
                        }
                    }
                }

            for(HashSet<String> key : lineTriples){
                if(candidateTriples.containsKey(key)){
                    candidateTriples.put(key, candidateTriples.get(key) + 1);
                }
            }
        }
        for(HashSet<String> key : candidateTriples.keySet()){
            if(candidateTriples.get(key) >= THRESHOLD){
                frequentTriples.put(key, candidateTriples.get(key));
            }
        }
    }

    private static void computePairConfidence(HashMap<HashSet<String>, Integer> frequentPairs){
        for(HashSet<String> frequentPair : frequentPairs.keySet()){
            Iterator<String> iterator = frequentPair.iterator();
            String key1 = "";
            String key2 = "";
            while (iterator.hasNext()){
                key1 = iterator.next();
                key2 = iterator.next();
            }

            pairConfidence.put(key1 + " ⇒ " + key2, (double)frequentPairs.get(frequentPair)/frequentItems.get(key1));
            pairConfidence.put(key2 + " ⇒ " + key1, (double)frequentPairs.get(frequentPair)/frequentItems.get(key2));

        }
    }

    private static void computeTripleConfidence(HashMap<HashSet<String>, Integer> frequentTriples){
        for(HashSet<String> frequentTriple : frequentTriples.keySet()){
            Iterator<String> iterator = frequentTriple.iterator();
            String key1 = "";
            String key2 = "";
            String key3 = "";
            while (iterator.hasNext()){
                key1 = iterator.next();
                key2 = iterator.next();
                key3 = iterator.next();
            }

            HashSet<String> set1 = new HashSet<>();
            HashSet<String> set2 = new HashSet<>();
            HashSet<String> set3 = new HashSet<>();

            set1.add(key1);
            set1.add(key2);
            set2.add(key1);
            set2.add(key3);
            set3.add(key2);
            set3.add(key3);

            tripleConfidence.put("(" + key1 + ", " + key2 + ")" + " ⇒ " + key3, (double)frequentTriples.get(frequentTriple)/frequentPairs.get(set1));
            tripleConfidence.put("(" + key1 + ", " + key3 + ")" + " ⇒ " + key2, (double)frequentTriples.get(frequentTriple)/frequentPairs.get(set2));
            tripleConfidence.put("(" + key2 + ", " + key3 + ")" + " ⇒ " + key1, (double)frequentTriples.get(frequentTriple)/frequentPairs.get(set3));
        }
    }

    public static void main(String[] args) throws IOException {
        File file = new File(PATH);
        firstPass(file);
        System.out.println(frequentItems.size());
        secondPass(file);
        System.out.println(frequentPairs.size());
        computePairConfidence(frequentPairs);
        thirdPass(file);
        computeTripleConfidence(frequentTriples);
        System.out.println(frequentTriples.size());

        List<Map.Entry<String, Double>> list1 = new ArrayList<>(pairConfidence.entrySet());
        list1.sort((o1, o2) -> {
            double result = o2.getValue() - o1.getValue();
            if (result > 0)
                return 1;
            else if (result == 0)
                return 0;
            else
                return -1;
        });

        List<Map.Entry<String, Double>> list2 = new ArrayList<>(tripleConfidence.entrySet());
        list2.sort((o1, o2) -> {
            double result = o2.getValue() - o1.getValue();
            if (result > 0)
                return 1;
            else if (result == 0)
                return o1.getKey().compareTo(o2.getKey());
            else
                return -1;
        });


        BufferedWriter bufferedWriter1 = new BufferedWriter(new FileWriter(OUTPUT1));
        for(Map.Entry<String, Double> set: list1){
            bufferedWriter1.write(set.getKey() + "\t" + set.getValue() + "\n");
        }
        bufferedWriter1.close();

        BufferedWriter bufferedWriter2 = new BufferedWriter(new FileWriter(OUTPUT2));
        for(Map.Entry<String, Double> set: list2){
            bufferedWriter2.write(set.getKey() + "\t" + set.getValue() + "\n");
        }
        bufferedWriter2.close();

        System.out.println("Hello World!");
    }
}
