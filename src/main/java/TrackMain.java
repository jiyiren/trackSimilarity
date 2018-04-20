import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TrackMain {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        if(args.length != 2){
            System.err.print("Usage: MaxSimilarity <input path> <output path>");
            System.exit(-1);
        }
        String cache = "hdfs://localhost:9000/jiyi/target/target_data";
        cache = cache+"#target_data";
        Job job = Job.getInstance();
        job.setJarByClass(TrackMain.class);
        job.setJobName("Max Similarity");
        job.addCacheFile(new URI(cache));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TrackMapper.class);
        job.setReducerClass(TrackReducer.class);

        // 设置mapper的输出key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置reducer的输出key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
