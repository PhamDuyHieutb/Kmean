package kmean;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeanMapper extends Mapper<Object, Text, Text, Text> {

	ArrayList<Point> center = new ArrayList<Point>();
	int count = 0;

	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		count = Integer.parseInt(conf.get("iterator"));
		
		
		if (count == 0) {
			Path centroid = new Path(conf.get("centroid"));
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroid)));
			String line = br.readLine();
			while (line != null) {
				center.add(new Point(line));
				line = br.readLine();
			}
			br.close();
		} else {
			Path out = new Path("/hkmean/out" + (count - 1) + "/part-r-00000");
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(out)));
			String line = br.readLine();
			while (line != null) {
				center.add(new Point(line));
				line = br.readLine();
			}
			br.close();
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Point p = new Point(value.toString());
		Point nearest = center.get(0);
		double mindict = Double.MAX_VALUE;
		for (Point c : center) {
			double dict = p.dictanceToCent(c);
			if (dict < mindict) {
				mindict = dict;
				nearest = c;
			}
		}
		context.write(new Text(nearest.toString()), value);

	}
}
