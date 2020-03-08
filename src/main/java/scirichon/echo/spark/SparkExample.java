package scirichon.echo.spark;

import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class SparkExample {

	public static void main(String[] args) {
		System.out.println(URI.create("39.100.238.90").getHost());
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local"); // sparkӦ�ó���Ҫ���ӵ�spark��Ⱥ��master�ڵ��url��local������Ǳ�������
		// .setMaster("spark://ip:port");

		s3select(conf);

//		localDataSet(conf);
		
//		rddTest(conf);

	}

	private static void s3select(SparkConf conf) {
		SparkSession session = SparkSession.builder().config(conf).config("spark.speculation", "false")
				.config("spark.network.timeout", "600s").config("spark.sql.codegen.wholeStage", "false")
				.config("spark.executor.heartbeatInterval", "500s")
				.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.config("mapreduce.fileoutputcommitter.algorithm.version", "2")
				.config("fs.s3a.connection.establish.timeout", "501000").config("fs.s3a.connection.timeout", "501000")
				.getOrCreate();

		session.sparkContext().setLogLevel("WARN");
		session.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "echo");
		session.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "echo1231");
		session.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "http://39.100.238.90:8001");
		session.sparkContext().hadoopConfiguration().set("fs.s3a.path.style.access", "true");
		session.sparkContext().hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");
		session.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

//LocID,Location,VarID,Variant,Time,MidPeriod,PopMale,PopFemale,PopTotal
		StructType structType = schema();

		Column column = new Column("Time");
		Dataset<Row> dsDataset = session.read().schema(structType).format("minioselectCSV").option("header", "true")
				.option("compression", "gzip").load("s3://mycsvbucket/sampledata/TotalPopulation.csv.gz");

//		Dataset<Row> dsDataset = session.read().option("header", "true")
//				.csv("s3a://mycsvbucket/sampledata/TotalPopulation.csv");

		String QUERY = "select s.Location from s3object s where s.Location like '%United States%'";
		dsDataset.printSchema();
//		dsDataset.sqlContext().sql(QUERY).show();
		dsDataset.select(column).filter("Time > 2089").show();
		// For CSV File
		dsDataset.select(column).filter("Time > 2089").write().csv("s3a://mycsvbucket/sampledata/location.csv");
		// For Json File
//		session.read().format("s3selectJSON").load("s3a://bucket/key")
//		.select(org.apache.spark.sql.functions.col("Location")).write().csv("s3a://bucket/key");
	}

	private static StructType schema() {

		StructField[] structFields = new StructField[] {
				new StructField("LocID", DataTypes.StringType, true, Metadata.empty()),
				new StructField("Location", DataTypes.StringType, true, Metadata.empty()),
				new StructField("VarID", DataTypes.StringType, true, Metadata.empty()),
				new StructField("Variant", DataTypes.StringType, true, Metadata.empty()),
				new StructField("Time", DataTypes.LongType, true, Metadata.empty()),
				new StructField("MidPeriod", DataTypes.StringType, true, Metadata.empty()),
				new StructField("PopMale", DataTypes.StringType, true, Metadata.empty()),
				new StructField("PopFemale", DataTypes.StringType, true, Metadata.empty()),
				new StructField("PopTotal", DataTypes.StringType, true, Metadata.empty()) };

		StructType structType = new StructType(structFields);

		return structType;
	}

	private static void localDataSet(SparkConf conf) {
		SparkSession session = SparkSession.builder().config(conf).config("spark.speculation", "false")
				.config("spark.network.timeout", "600s").config("spark.sql.codegen.wholeStage", "false")
				.config("spark.executor.heartbeatInterval", "500s")
				.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.config("mapreduce.fileoutputcommitter.algorithm.version", "2")
				.config("fs.s3a.connection.establish.timeout", "501000").config("fs.s3a.connection.timeout", "501000")
				.getOrCreate();

//LocID,Location,VarID,Variant,Time,MidPeriod,PopMale,PopFemale,PopTotal
		StructType structType = schema();

		Column column = new Column("Location");

		Dataset<Row> dsDataset = session.read().schema(structType).option("header", "true")
				.csv("e:/spark/TotalPopulation.csv");

		String QUERY = "select s.Location from s3object s where s.Location like '%United States%'";
		dsDataset.printSchema();
//		dsDataset.sqlContext().sql(QUERY).show();
		dsDataset.select("*").show();
		dsDataset.write().csv("e:/spark/hello.csv");
		// For CSV File
//		dsDataset.write().csv("s3a://mycsvbucket/sampledata/location.csv");
		// For Json File
//		session.read().format("s3selectJSON").load("s3a://bucket/key")
//		.select(org.apache.spark.sql.functions.col("Location")).write().csv("s3a://bucket/key");
	}

	private static void rddTest(SparkConf conf) {

//		StreamingContext
		// ����JavaSparkContext����

		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

		sc.hadoopConfiguration().set("fs.s3a.access.key", "echo");
		sc.hadoopConfiguration().set("fs.s3a.secret.key", "echo1231");
		sc.hadoopConfiguration().set("fs.s3a.endpoint", "http://39.100.238.90:8001");
		sc.hadoopConfiguration().set("fs.s3a.path.style.access", "true");
		sc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");
		sc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

		// �������Դ��hdfs�ļ��������ļ��ȣ�����һ����ʼ��RDD
//		JavaRDD<String> lines = sc.textFile("e://spark/sparkTestFile.txt");
//		sc.parallelize(list);
		JavaRDD<String> lines = sc.textFile("s3a://test/sparkTestFile.txt");
		lines.saveAsTextFile("s3a://test/sparkTestFile_copy2.txt");
		// �Գ�ʼRDD����transformation��������flatMap��mapToPair��reduceByKey

		// ��ÿһ�в�ֳɵ����ĵ���
		// FlatMapFunction���������Ͳ����������������������
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});

		// ��Ҫ��ÿһ������ӳ��Ϊ�����ʣ�1���ĸ�ʽ
		// JavaPairRDD����������������TupleԪ�صĵ�һ��ֵ�͵ڶ���ֵ
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// ��Ҫ�Ե�����Ϊkey��ͳ��ÿ�����ʳ��ֵĴ���
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		wordCounts.saveAsTextFile("s3a://test/sparkTestWorldCount2");

//		wordCounts.mapToPair(v -> new Tuple2<Text, IntWritable>(new Text(v._1()), new IntWritable(v._2())))
//				.saveAsTextFile("e://spark/sparkTestWorldCount.txt");

//		wordCounts.mapToPair(v -> new Tuple2<Text, IntWritable>(new Text(v._1()), new IntWritable(v._2()))).sortByKey()
//				.saveAsNewAPIHadoopFile("s3a://test/sparkTestWorldCount", Text.class, IntWritable.class, MapFileOutputFormat.class);

		// foreach��������ִ��
		wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
			}
		});

		sc.stop();
	}
}
