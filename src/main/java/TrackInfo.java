public class TrackInfo {
    public double similarity;
    public String trackId;

    TrackInfo(String trackId,double similarity){
        this.trackId=trackId;
        this.similarity = similarity;
    }

    @Override
    public String toString() {
        return trackId + "," + similarity;
    }
}
