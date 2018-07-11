
/* SimpleApp.java */
/*This Program is used to Count the degree of separation 
 * between startCharacterID and targetCharacterID in the marvel universe
 * It use breath first search algorithm to search target hero id*/
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.LongAccumulator;

public class SimpleApp {
	public static SparkConf conf = new SparkConf();
	public static int startCharacterID = 5306;// spider man, change this to whatever you want
	public static int targetCharacterID = 6459;// change this to whatever you want
	public static String logFile = "D:\\SparkCourse\\Marvel-Graph.txt"; //This should be the file path in your pc
	public static LongAccumulator hitted;//This is used as flag whether the target has hitted

	// Main Program
	public static void main(String[] args) {
		
		//Get Started RDD
		JavaPairRDD<Integer, Tuple3> rdd = createStartingRdd();

		System.out.println("Make sure Accumulaotr is zero?" + hitted.isZero());

		// We assume that it will not succed 10 degree to run all social graph
		for (int i = 0; i <= 10; i++) {

			// Run BSF MAP
			JavaPairRDD<Integer, Tuple3> mapped = rdd
					.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple3>, Integer, Tuple3>() {
						@Override
						public Iterator<Tuple2<Integer, Tuple3>> call(Tuple2<Integer, Tuple3> a) throws Exception {
							return bfsMap(a);
						}
					});

			System.out.println("Processing " + String.valueOf(mapped.count()) + " values.");

			// Check weather it is already find it
			if (hitted.value() > 0) {
				System.out.println("==================================");
				System.out.println("Finded!!!!!!!!!!!!, The degree of separation should be " + hitted.value());
				System.out.println("==================================");
				break;
			}
			
			// Run BSF Reduce
			rdd = mapped.reduceByKey(new Function2<Tuple3, Tuple3, Tuple3>() {
				@Override
				public Tuple3 call(Tuple3 a, Tuple3 b) throws Exception {
					return bfsReduce(a, b);
				}
			});

		}
	}

	// Create the Started RDD,return Tuple2 (HeroId,(Connections,Distance,Color))
	public static JavaPairRDD<Integer, Tuple3> createStartingRdd() {
		JavaSparkContext sc = new JavaSparkContext(conf);
		hitted = new LongAccumulator();
		JavaRDD<String> inputFile = sc.textFile(logFile);
		return inputFile.mapToPair(new PairFunction<String, Integer, Tuple3>() {
			public Tuple2<Integer, Tuple3> call(String s) {
				return convertToBFSz(s);
			}
		});
	}

	// This FN will be called by createStartingRdd(Above FN)
	public static Tuple2<Integer, Tuple3> convertToBFSz(String s) {
		String[] ids = s.split(" ");
		Integer heroId = Integer.valueOf(ids[0]);
		List<Integer> connections = new ArrayList<Integer>();
		for (int i = 1; i <= ids.length - 1; i++) {
			connections.add(Integer.valueOf(ids[i]));
		}
		String color = "WHITE";
		int distance = 9999;
		if (heroId == startCharacterID) {
			color = "GRAY";
			distance = 0;
		}
		Tuple3 field2 = new Tuple3(connections, distance, color);
		Tuple2<Integer, Tuple3> rdd = new Tuple2<Integer, Tuple3>(heroId, field2);
		return rdd;
	}

	// BFS Map function
	public static Iterator<Tuple2<Integer, Tuple3>> bfsMap(Tuple2<Integer, Tuple3> node) {
		int characterID = Integer.valueOf(node._1);
		Tuple3 data = node._2;
		ArrayList<Integer> connections = (ArrayList<Integer>) data._1();
		int distance = (int) data._2();
		String color = (String) data._3();
		ArrayList<Tuple2<Integer, Tuple3>> results = new ArrayList<Tuple2<Integer, Tuple3>>();

		if (color.equals("GRAY")) {
			for (int connection : connections) {
				int newCharacterID = connection;
				int newDistance = distance + 1;
				String newColor = "GRAY";
				if (targetCharacterID == connection) {
					hitted.add(1);
				}

				Tuple2 newEntry = new Tuple2(newCharacterID,new Tuple3(new ArrayList<Integer>(), newDistance, newColor));
				results.add(newEntry);
			}
			color = "BLACK";
		}
		results.add(new Tuple2(characterID, new Tuple3(connections, distance, color)));

		Iterator<Tuple2<Integer, Tuple3>> itr = results.iterator();
		return itr;
	}

	// BFS Reduce function
	public static Tuple3 bfsReduce(Tuple3 data1, Tuple3 data2) {
		ArrayList<Integer> edges1 = (ArrayList<Integer>) data1._1();
		ArrayList<Integer> edges2 = (ArrayList<Integer>) data2._1();
		int distance1 = (int) data1._2();
		int distance2 = (int) data2._2();
		String color1 = (String) data1._3();
		String color2 = (String) data2._3();

		int distance = 9999;
		String color = color1;
		ArrayList<Integer> edges = new ArrayList<Integer>();

		if (edges1.size() > 0) {
			edges.addAll(edges1);
		}
		if (edges2.size() > 0) {
			edges.addAll(edges2);
		}

		if (distance1 < distance) {
			distance = distance1;
		}
		if (distance2 < distance) {
			distance = distance2;
		}

		if (color1.equals("WHITE") && (color2.equals("GRAY") || color2.equals("BLACK"))) {
			color = color2;
		}
		if (color1.equals("GRAY") && color2.equals("BLACK")) {
			color = color2;
		}
		if (color2.equals("WHITE") && (color1.equals("GRAY") || color1.equals("BLACK"))) {
			color = color1;
		}
		if (color2.equals("GRAY") && color1.equals("BLACK")) {
			color = color1;
		}

		return new Tuple3(edges, distance, color);
	}

}