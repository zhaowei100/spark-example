package scirichon.echo.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Function1;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scirichon.echo.spark.vo.TbDate;
import scirichon.echo.spark.vo.TbStock;
import scirichon.echo.spark.vo.TbStockDetail;

public class SparkExample {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[2]"); // spark应用程序要连接的spark集群的master节点的url，local代表的是本地运行
		// .setMaster("spark://ip:port");

//		s3select(conf);

		execise(conf);

//		localDataSet(conf);

//		rddTest(conf);

	}

	private static void execise(SparkConf conf) {
		SparkSession session = minioSession(conf);

		Dataset<String> tbStockSet = session.read().textFile("s3a://mycsvbucket/sampledata/execise/tbStock.txt");
		Dataset<String> tbDateSet = session.read().textFile("s3a://mycsvbucket/sampledata/execise/tbDate.txt");
		Dataset<String> tbStockDetailSet = session.read()
				.textFile("s3a://mycsvbucket/sampledata/execise/tbStockDetail.txt");

		// 读取表1
		JavaRDD<TbStock> tbStockRdd = tbStockSet.javaRDD().map(new Function<String, TbStock>() {

			@Override
			public TbStock call(String v1) throws Exception {
				String s[] = v1.split(",");
				TbStock tbStock = new TbStock();
				tbStock.setOrderNumber(s[0]);
				tbStock.setLocationId(s[1]);
				tbStock.setDateId(s[2]);
				return tbStock;
			}
		});
		Dataset<Row> tbstockDataset = session.createDataFrame(tbStockRdd, TbStock.class);
		tbstockDataset.printSchema();
		tbstockDataset.show();
		
		// 读取表2
		JavaRDD<TbDate> tbDateRdd = tbDateSet.javaRDD().map(new Function<String, TbDate>() {

			@Override
			public TbDate call(String v1) throws Exception {
				String[] s = v1.split(",");
				TbDate p = new TbDate();
				p.setDateid(s[0]);
				p.setYears(Integer.valueOf(s[1]));
				p.setTheYear(Integer.valueOf(s[2]));
				p.setMonth(Integer.valueOf(s[3]));
				p.setDay(Integer.valueOf(s[4]));
				p.setWeekday(Integer.valueOf(s[5]));
				p.setWeek(Integer.valueOf(s[6]));
				p.setQuarter(Integer.valueOf(s[7]));
				p.setPeriod(Integer.valueOf(s[8]));
				p.setHalfMonth(Integer.valueOf(s[9]));
				return p;
			}
		});
		Dataset<Row> tbdataDataset = session.createDataFrame(tbDateRdd, TbDate.class);
		tbdataDataset.printSchema();
		tbdataDataset.show();
		
		// 读取表3
		JavaRDD<TbStockDetail> detailRdd = tbStockDetailSet.javaRDD().map(new Function<String, TbStockDetail>() {

			@Override
			public TbStockDetail call(String v1) throws Exception {
				String[] a = v1.split(",");
				TbStockDetail tbStockDetail = new TbStockDetail();
				tbStockDetail.setOrderNumber(a[0]);
				tbStockDetail.setRownum(Integer.valueOf(a[1]));
				tbStockDetail.setItemId(a[2]);
				tbStockDetail.setNumber(Integer.valueOf(a[3]));
				tbStockDetail.setPrice(Double.valueOf(a[4]));
				tbStockDetail.setAmount(Double.valueOf(a[5]));
				return tbStockDetail;
			}
		});
		
		Dataset<Row> detailDataset = session.createDataFrame(detailRdd, TbStockDetail.class);
		detailDataset.printSchema();
		detailDataset.show();
		
		// 注册表
		tbstockDataset.createOrReplaceTempView("tbStock");
		tbdataDataset.createOrReplaceTempView("tbDate");
		detailDataset.createOrReplaceTempView("tbStockDetail");
		
		
		session.stop();
	}

	private static void s3select(SparkConf conf) {
		SparkSession session = minioSession(conf);

//LocID,Location,VarID,Variant,Time,MidPeriod,PopMale,PopFemale,PopTotal
		StructType structType = schema();

		Column column = new Column("Time");
		Dataset<Row> dsDataset = session.read().schema(structType).format("minioselectCSV").option("header", "true")
				.option("compression", "gzip").load("s3://mycsvbucket/sampledata/TotalPopulation.csv.gz");

//		Dataset<Row> dsDataset = session.read().option("header", "true")
//				.csv("s3a://mycsvbucket/sampledata/TotalPopulation.csv");

		// sql
		dsDataset.createOrReplaceTempView("population");
		String QUERY = "select Time from population s where Time > 2099";
		dsDataset.printSchema();
		dsDataset.sqlContext().sql(QUERY).show();
		dsDataset.sqlContext().sql(QUERY).write().csv("s3a://mycsvbucket/sampledata/location_sql.csv");

		// 普通方式
//		dsDataset.select(column).filter("Time > 2099").show();
//		dsDataset.select(column).filter("Time > 2099").write().csv("s3a://mycsvbucket/sampledata/location.csv");

		// For Json File
//		session.read().format("s3selectJSON").load("s3a://bucket/key")
//		.select(org.apache.spark.sql.functions.col("Location")).write().csv("s3a://bucket/key");
		session.close();
	}

	private static SparkSession minioSession(SparkConf conf) {
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
		return session;
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
		session.close();
	}

	private static void rddTest(SparkConf conf) {

//		StreamingContext
		// 创建JavaSparkContext对象

		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

		sc.hadoopConfiguration().set("fs.s3a.access.key", "echo");
		sc.hadoopConfiguration().set("fs.s3a.secret.key", "echo1231");
		sc.hadoopConfiguration().set("fs.s3a.endpoint", "http://39.100.238.90:8001");
		sc.hadoopConfiguration().set("fs.s3a.path.style.access", "true");
		sc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");
		sc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

		// 针对输入源（hdfs文件、本地文件等）创建一个初始的RDD
//		JavaRDD<String> lines = sc.textFile("e://spark/sparkTestFile.txt");
//		sc.parallelize(list);
		JavaRDD<String> lines = sc.textFile("s3a://test/sparkTestFile.txt");
		lines.saveAsTextFile("s3a://test/sparkTestFile_copy2.txt");
		// 对初始RDD进行transformation操作，如flatMap、mapToPair、reduceByKey

		// 将每一行拆分成单个的单词
		// FlatMapFunction的两个泛型参数代表了输入输出的类型
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});

		// 需要将每一个单词映射为（单词，1）的格式
		// JavaPairRDD的两个参数代表了Tuple元素的第一个值和第二个值
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// 需要以单词作为key，统计每个单词出现的次数
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

		// foreach触发程序执行
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
