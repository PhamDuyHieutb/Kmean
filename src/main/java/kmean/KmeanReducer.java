package kmean;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeanReducer extends Reducer<Text, Text, Text, Text> {


	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		ArrayList<Point> listpoint = new ArrayList<Point>();
		FileSystem fs = FileSystem.get(conf);
		double chance = 0;

		Path chanc = new Path("/hkmean/chance");
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(chanc)));
		chance = Double.parseDouble(br.readLine());
		br.close();

		for (Text input : values) {
			String[] temp = input.toString().split(" ");
			listpoint.add(new Point(Double.parseDouble(temp[0]), Double.parseDouble(temp[1])));
		}
		double sumX = 0, sumY = 0;
		double size = listpoint.size();
		for (Point p : listpoint) {

			sumX += p.getX();
			sumY += p.getY();
		}

		sumX = sumX / size;
		sumY = sumY / size;
		Point newcentroid = new Point(sumX, sumY);
		Point prevcentroid = new Point(key.toString());

		chance += prevcentroid.dictanceToCent(newcentroid);

		Path outchance = new Path("/hkmean/chance");
		OutputStream os = fs.create(outchance);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
		bw.write(chance + "");
		bw.close();

		context.write(new Text(newcentroid.toString()), new Text(""));

	}

}
