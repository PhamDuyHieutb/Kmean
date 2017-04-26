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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KmeanTest {
	

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Path input = new Path("/datakmean/data.txt");
		Path centroid = new Path("/datakmean/Centroid.txt");
		int iterator= 0;
		double chance =0;
		int numCentroid = 3;
		
	    boolean isdone = false;
	    while(!isdone){
	    	Configuration conf = new Configuration();
	    	conf.set("iterator", Integer.toString(iterator));
			conf.set("chance",Double.toString(chance));
			conf.set("centroid", centroid.toString());
			conf.set("numCentroid", Integer.toString(numCentroid));
			
			Job job = Job.getInstance(conf);
			job.setJobName("Kmean_job"+ iterator);
			
			
		    job.setJarByClass(KmeanMapper.class);
		    job.setMapperClass(KmeanMapper.class);
		    job.setReducerClass(KmeanReducer.class);
		    
		    Path output = new Path("/Kmean1/out"+iterator);
		    FileSystem fs = FileSystem.get(conf);
		    if(fs.exists(output)) {
		    	fs.delete(output,true);
		    }
		    
		    FileInputFormat.addInputPath(job, input);
		    FileOutputFormat.setOutputPath(job, output);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    job.waitForCompletion(true);
		    
		    Path chanc= new Path("/datakmean/chance");
		    FileSystem fs1 = FileSystem.get(new Configuration());
		    BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(chanc)));
//		    ArrayList<Point> newcentroids = new ArrayList<Point>();
//		    String l = br1.readLine();
//		    while(l!=null){
//		    	String[] a =l.split("");
//		    	double x= Double.parseDouble(a[0]);
//		    	double y= Double.parseDouble(a[1]);
//		    	newcentroids.add(new Point(x,y));
//		    }
		    
		    chance = Double.parseDouble(br1.readLine());
		    if(chance < numCentroid*10)
		    	isdone = true;
		   
		    	Path outchance = new Path("/datakmean/chance");
		    	OutputStream os = fs.create(outchance);
		    	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
		    	bw.write("0.0");
		    	bw.close();
		    	iterator++;
		    

	    }
	  
		
		
	}

}
