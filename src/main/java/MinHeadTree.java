import java.text.DecimalFormat;

public class MinHeadTree {
    public TrackInfo[] trackInfos;
    int size = 10;
    int curIndex = 0;
    DecimalFormat df = new DecimalFormat("0.0000");

    MinHeadTree(int size){
        if(size<=0){
            this.size = 10;
        }else{
            this.size = size;
        }
        trackInfos = new TrackInfo[this.size];
    }

    public int getSize(){
        return size;
    }

    public boolean addElement(TrackInfo trackInfo){
        if(curIndex<size){
            trackInfos[curIndex++] = trackInfo;
            minAdjust(0,curIndex-1);
            return true;
        }else{
            //将最小值替换
            TrackInfo head = getHead();
            if(head!=null && (trackInfo.similarity>head.similarity)){
                trackInfos[0] = trackInfo;
                minAdjust(0,curIndex-1);
                return true;
            }else{
                return false;
            }
        }
    }

    public TrackInfo getHead(){
        if(trackInfos.length>=size){
            return trackInfos[0];
        }
        return null;
    }

    public void minAdjust(int startIndex,int endIndex){
        if(startIndex*2+1>endIndex){
            return;//无儿子节点则返回
        }
        minAdjust(2*startIndex+1,endIndex);
        minAdjust(2*startIndex+2,endIndex);
        int minIndex = 2*startIndex+1;
        TrackInfo trackInfo = null;
        if((2*startIndex+2<=endIndex) && (trackInfos[2*startIndex+2].similarity<trackInfos[2*startIndex+1].similarity)){
            minIndex = 2*startIndex+2;
        }
        //根节点与最大儿子比较，小于儿子则交换
        if(trackInfos[startIndex].similarity>trackInfos[minIndex].similarity){
            trackInfo = trackInfos[startIndex];
            trackInfos[startIndex] = trackInfos[minIndex];
            trackInfos[minIndex] = trackInfo;
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (TrackInfo trackInfo:trackInfos){
            sb.append(trackInfo.trackId+","+df.format(trackInfo.similarity)+" ");
        }
        return sb.toString();
    }
}
