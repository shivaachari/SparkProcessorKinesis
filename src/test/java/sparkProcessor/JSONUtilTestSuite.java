package sparkProcessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.spark.sql.Row;
import org.junit.Test;

import com.hotstar.datamodel.streaming.spark.util.JSONUtil;

public class JSONUtilTestSuite {

	Path path;
	static String data;


	@Test
	public void test() {
		try {
			/*String data = "{\"os\":{\"osName\":\"android\",\"osVersion\":\"5.1\"},\"app\":{\"appName\":\"hotstar\",\"appVersion\":\"5.9.4\"},\"content\":{\"pageRef\":\"hot.home.featured.index\"}}" +
					   System.lineSeparator() +
					   "{\"os\":{\"osName\":\"android\",\"osVersion\":\"5.0.2\"},\"app\":{\"appName\":\"hotstar\",\"appVersion\":\"5.9.0\"},\"player\":{\"playerName\":\"android_puppet\",\"playerVersion\":\"2,4,0,61610\"},\"content\":{\"contentId\":\"1000113248\",\"name\":\"Khoka Babu-Khoka is a Waiter!-182\",\"url\":\"https://staragvod3-vh.akamaihd.net/i/videos/jalsha/kb/182/1000113248_,16,54,106,180,400,800,1300,2000,3000,4500,_STAR.mp4.csmil/master.m3u8?hdnea\u003dst\u003d1478587796~exp\u003d1478588396~acl\u003d/*~hmac\u003d2ee4108d5ab6d6df8c553abe95d86a3ed95572ef494666ad21cd3a9b34280ee1\",\"mode\":\"vod\",\"contentType\":\"non-premium\",\"pageRef\":\"hot.tvshows.Khoka Babu.season4.Khoka is a Waiter!.episodedetail\",\"clipBitrate\":\"0\"},\"events\":[{\"eventType\":\"playerEvent\",\"event\":{\"timestamp\":\"1478591351999\",\"eventCounter\":\"4\",\"playerEvent\":\"keep-alive\",\"position\":\"1210093\",\"videoSegment\":\"1\",\"bufferingTime\":\"59\",\"videoUrl\":\"https://staragvod3-vh.akamaihd.net/i/videos/jalsha/kb/182/1000113248_,16,54,106,180,400,800,1300,2000,3000,4500,_STAR.mp4.csmil/master.m3u8?hdnea\u003dst\u003d1478587796~exp\u003d1478588396~acl\u003d/*~hmac\u003d2ee4108d5ab6d6df8c553abe95d86a3ed95572ef494666ad21cd3a9b34280ee1\",\"contentId\":\"1000113248\",\"programTitle\":\"Khoka Babu\",\"episodeTitle\":\"Khoka is a Waiter!\"}}]}";
								 */      
			Path path = Paths.get("/Users/shivaa/git/SparkProcessorKinesis_git/src/main/resources/datamodel/streaming/inputs/clientevent.json");
			   data = new String(Files.readAllBytes(path));
			ArrayList<Row> array = new ArrayList<Row>();
			JSONUtil.ParseStringJSON(data, array);
			
			
			for(Row row : array) {
				System.out.println(row.toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	public static void main(String[] args) {
		
	}
	
	

}
