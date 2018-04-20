import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrackReducer extends Reducer<Text,Text,Text,Text> {


    private MinHeadTree minHeadTree = new MinHeadTree(10);
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value:values){
//            String curTrackId = value.toString().split(",")[0];
            double curSim = Double.valueOf(value.toString().split(",")[1]);
            minHeadTree.addElement(new TrackInfo(value.toString().split(",")[0],curSim));
        }
        context.write(key,new Text(minHeadTree.toString()));
    }
}
