import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import java.util.StringTokenizer;

public class PreProcessing {
	public static class PreProcessMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: XML String of the format <row key="value" />

			StringTokenizer attributeTokens = new StringTokenizer(
					value.toString());

			String postTypeId = null;
			String parentId = null;
			String ownerUserId = null;

			while (attributeTokens.hasMoreTokens()) {

				String[] keyValue;

				String attributeKeyValue = (String) attributeTokens.nextToken();
				if (attributeKeyValue.contains("PostTypeId")) {
					keyValue = attributeKeyValue.split("[\"]");
					postTypeId = keyValue[1];
					if (postTypeId.equals("2")) {
						continue;
					} else
						break;
				}
				if (attributeKeyValue.contains("ParentId")) {
					keyValue = attributeKeyValue.split("[\"]");
					parentId = keyValue[1];
				} else if (attributeKeyValue.contains("OwnerUserId")) {
					keyValue = attributeKeyValue.split("[\"]");
					ownerUserId = keyValue[1];

					try {
						if (parentId != null && ownerUserId != null) {
							output.collect(null, new Text(parentId + ","
									+ ownerUserId));
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			// Output: QuestionID, UserID

		}
	}
}
