import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TrackMapper extends Mapper<LongWritable,Text,Text,Text>{
    private static final String fileName = "target_data";
    // LCSS算法待确定参数
    private double e = 0.01;              // 两点之间的距离
    private double o = 20;                // 定义相邻的最长距离不超过20个点
//    private final String targetTrack = "10342,56,1,24.06331,77.166323,1,24.063021,77.164736,1,24.064109,77.163311,1,24.063576,77.163084,1,24.063901,77.162736,1,24.064197,77.161492,1,24.063285,77.161255,1,24.063338,77.160783,1,24.063706,77.16198,1,24.064036,77.161946,1,24.063919,77.160101,1,24.064172,77.159405,1,24.063241,77.158836,1,24.062363,77.158576,1,24.062841,77.158419,1,24.061536,77.158471,1,24.059369,77.156843,1,24.059344,77.156038,1,24.059909,77.15562,1,24.060663,77.15472,1,24.061825,77.15302,1,24.063335,77.151612,1,24.064303,77.151611,1,24.062345,77.151072,1,24.061972,77.150877,1,24.063282,77.149868,1,24.062483,77.149442,1,24.063779,77.148707,1,24.063256,77.147918,1,24.063123,77.147148,1,24.063751,77.144831,1,24.064381,77.144827,1,24.064132,77.144433,1,24.064764,77.144376,1,24.064773,77.143281,1,24.067328,77.143423,1,24.068646,77.143458,1,24.069927,77.142775,1,24.069028,77.140432,1,24.068318,77.140029,1,24.068596,77.138902,1,24.067749,77.138728,1,24.067531,77.13838,1,24.066876,77.137678,1,24.064997,77.13709,1,24.063131,77.135389,1,24.063309,77.134202,1,24.063896,77.134572,1,24.060927,77.134053,1,24.061616,77.133692,1,24.06119,77.133621,1,24.060675,77.13343,1,24.059773,77.131479,1,24.059895,77.131279,1,24.060218,77.128556,1,24.06042,77.12803";
    private List<String> targetTracks;
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        targetTracks = new ArrayList<>();
        FileReader fr = new FileReader(fileName);
        BufferedReader bufferedReader = new BufferedReader(fr);
        String line = null;
        while ((line = bufferedReader.readLine())!=null){
            targetTracks.add(line);
        }
        bufferedReader.close();
        fr.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for (String targetTrack:targetTracks){
            String curTrack = value.toString();
            String[] targetStr = targetTrack.split(",");
            String[] curStr = curTrack.split(",");
            if(targetStr[0].equals(curStr[0])){
                return;
            }
            int m = Integer.valueOf(targetStr[1]);                                         // 获取矩阵A的长度
            reset_e_o(targetStr, m);                                                       // 根据新点编号的轨迹重新定义值e和值o
            double similarity = result(targetStr,curStr);
            if (similarity>0){
                System.out.println("e="+e+" o="+o);
                System.out.println(curStr[0]+":"+similarity);
            }
            outKey.set(targetStr[0]);
            outValue.set(curStr[0]+","+similarity);
            context.write(outKey,outValue);
        }
    }

    // 返回两条轨迹的相似度的结果
    public double result(String[] A, String[] B)
    {
        int m = A.length/3;                        // A轨迹对的个数
        int n = B.length/3;                        // B轨迹对的个数
        int[][] c = new int[m+1][n+1];             // 矩阵表

        // 遍历整个m*n的矩阵
        for (int i = 1; i <= m; i++)
        {
            for (int j = 1; j <= n; j++)
            {
                // 两点之间的纬经度足够短,并两点之间的距离不超过20
                if ( (Math.abs(Double.valueOf(A[3*i])-Double.valueOf(B[3*j])) < e)
                        && (Math.abs(Double.valueOf(A[3*i+1])-Double.valueOf(B[3*j+1])) < e)
                        && (Math.abs(i - j) <= o) )
                {
                    c[i][j] = c[i-1][j-1] + 1;            // 左上角的值加1
                }
                else if (c[i-1][j] >= c[i][j-1])          // 上方的值大于左边的值
                {
                    c[i][j] = c[i-1][j];                  // 参考正上方的值
                }
                else                                      // 左边的值大于上方的值
                {
                    c[i][j] = c[i][j-1];                  // 参考左边的值
                }
            }
        }
        // c[m][n]的值为相似点的个数
        // 相似度=相似点的个数/两条轨迹的最少点个数
        return ( c[m][n] * 2. / (m + n) );
    }

    // 重新定义最短距离e和最少点数o
    public void reset_e_o(String[] A, int m)
    {
        double sum = 0;                             // 此编号轨迹的长度
        for (int p=3; p<3*m; p += 3)                // 遍历轨迹点求e值
        {
            // 计算两点之间的欧式距离
            sum += Math.sqrt((Math.pow((Double.valueOf(A[p]) - Double.valueOf(A[p+3])), 2) +
                    Math.pow((Double.valueOf(A[p+1]) - Double.valueOf(A[p+4])), 2)));
        }
        e = sum / (m-1) * 10;                        // 重新定义最短距离
        o = m;                                  // 重新定义最少点数
    }
}
