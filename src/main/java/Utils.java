import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Utils {

	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
					.split("\"");

			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];

				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	// This helper function cleans tags and gives a string separated list of
	// tags.
	
	public static String cleanTags(String tags) {
		
		String cleaned = tags.replace("&gt;&lt;", ",");
		cleaned = cleaned.replace("&lt;", "");
		cleaned = cleaned.replace("&gt;", "");
		return cleaned;
	}
	
	// Helper to print a hashmap
	
	public static void printHashMap(HashMap <String, HashSet<String>> hm){
		
		for (Map.Entry entry : hm.entrySet()) {
		    System.out.println("key: "+entry.getKey());
		    HashSet<String> hs = hm.get(entry.getKey());
		    for(String value : hs){
		    	System.out.println("value: "+value);
		    }
		}
	
	}
	
}
