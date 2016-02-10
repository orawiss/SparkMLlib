package hospitalRecommendationF;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import scala.runtime.AbstractFunction0;


import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;



public class SparkOracle extends AbstractFunction0<Connection> implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Author: Wissem EL KHLIFI
	 */
	
	 public static final String DBURL = "jdbc:oracle:thin:@xxx.com:1521:hospital";
	 public static final String DBUSER = "wissem";
	 public static final String DBPASS = "wissem123";
	 
	public static void sparkOracle(JavaSparkContext sc,
			final SQLContext sqlctx) throws FileNotFoundException,
			UnsupportedEncodingException, SQLException  {

		// /get today date
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");
		Date now = new Date();
		String strDate = sdfDate.format(now);
		String inputFile = "hdfs://spark-01:8020/tmp/in.csv";
		PrintWriter writer90 = new PrintWriter(inputFile, "UTF-8");
		
		// Load Oracle JDBC Driver
        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        
        // Connect to Oracle Database
        Connection con = DriverManager.getConnection(DBURL, DBUSER, DBPASS);

        Statement statement = con.createStatement();

        // Execute a SELECT query on Oracle 

        ResultSet rs = statement.executeQuery(
        		    " SELECT hrt.rate.SubCategory, "+
							"hrt.rate.Hospital.HospitalID,"+
							"hrt.rate.Hospital.HospName,"+
							"hrt.rate.Rating.RatingID,"+
							"hrt.rate.Rating.RatingValue,"+
							"hrt.rate.UserInfo.UserName,"+
							"hrt.rate.UserInfo.UserID,"+
							"hrt.rate.UserInfo.UserCity,"+
							"hrt.rate.Hospital.HospCity,"+
							"hrt.rate.UserInfo.UserCountry,"+
							"hrt.rate.Hospital.HospCountry        "+
					   "FROM HospitalRating hrt");
        
          
        
                while (rs.next()) {	
                	writer90.println(rs.getString(1)+","+ rs.getString(2)+","+rs.getString(3)+","+
                          			rs.getString(4)+","+rs.getString(5)+","+rs.getString(6)+","+rs.getString(7)+","+rs.getString(8)
                        			+","+rs.getString(9)+","+rs.getString(10)+","+rs.getString(11));
       
     
                }

			
       writer90.close();
     
        
        
		JavaRDD<String> rawUserHospitalData = sc.textFile(inputFile);
		

		JavaRDD<Tuple2<Integer, String>> rawHosp = rawUserHospitalData
				.flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Integer, String>> call(String s) {

						// System.out.println("s::"+s);
						String[] sarray = s.replaceAll("\\[|\\]", " ").split(
								",");
						List<Tuple2<Integer, String>> returnList = new ArrayList<Tuple2<Integer, String>>();
						if (sarray.length >= 2) {
							try {
								if (Utils.isInteger(sarray[1])) {
									returnList.add(new Tuple2<Integer, String>(
											Integer.parseInt(sarray[1]),
											sarray[8].concat("-" + sarray[10])
													.trim()));
									return returnList;
								} else {
									returnList.add(new Tuple2<Integer, String>(
											-1, "NA"));
									return returnList;
								}
							} catch (NumberFormatException e) {
								e.printStackTrace();
								returnList.add(new Tuple2<Integer, String>(-1,
										"NA"));
								return returnList;
							}
						} else {
							try {
								returnList.add(new Tuple2<Integer, String>(-1,
										"NA"));
								return returnList;
							} catch (NumberFormatException e) {
								e.printStackTrace();
								returnList.add(new Tuple2<Integer, String>(-1,
										"NA"));
								return returnList;
							}
						}
					}
				});

	
		JavaPairRDD<Integer, String> rawHospRDD = JavaPairRDD
				.fromJavaRDD(rawHosp);

		// Convert the Tuple5 to elements of class Hospital
		JavaRDD<CityCountry> tableCityCountry = rawHospRDD
				.map(new Function<Tuple2<Integer, String>, CityCountry>() {
					private static final long serialVersionUID = 1L;

					public CityCountry call(Tuple2<Integer, String> v1)
							throws Exception {

						CityCountry st = new CityCountry(v1._1(), v1._2());
						return st;

					}
				});

		// // RDD UserID, HospID

		JavaRDD<Tuple2<Integer, String>> rawHospUser = rawUserHospitalData
				.flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Integer, String>> call(String s) {
						String[] sarray = s.replaceAll("\\[|\\]", " ").split(
								",");
						List<Tuple2<Integer, String>> returnList = new ArrayList<Tuple2<Integer, String>>();
						if (sarray.length >= 2) {
							try {
								if (Utils.isInteger(sarray[1])) {
									returnList.add(new Tuple2<Integer, String>(
											Integer.parseInt(sarray[6]),
											addQuotes(Integer
													.parseInt(sarray[1]))));
									return returnList;
								} else {
									returnList.add(new Tuple2<Integer, String>(
											-1, "-1"));
									return returnList;
								}
							} catch (NumberFormatException e) {
								e.printStackTrace();
								returnList.add(new Tuple2<Integer, String>(-1,
										"-1"));
								return returnList;
							}
						} else {
							try {
								returnList.add(new Tuple2<Integer, String>(-1,
										"-1"));
								return returnList;
							} catch (NumberFormatException e) {
								e.printStackTrace();
								returnList.add(new Tuple2<Integer, String>(-1,
										"-1"));
								return returnList;
							}
						}
					}
				});

		JavaPairRDD<Integer, String> rawHospUserRDD = JavaPairRDD
				.fromJavaRDD(rawHospUser);
		final Broadcast<JavaPairRDD<Integer, String>> rawHospUserB = sc
				.broadcast(rawHospUserRDD.cache());

		
		// /// Big loop on City Countries

		try {
			DataFrame dfCityCountry = sqlctx.createDataFrame(tableCityCountry,
					CityCountry.class);
			dfCityCountry.registerTempTable("CityCountry");

			DataFrame distinctCityCountry = sqlctx
					.sql("select hospCityCountry FROM CityCountry group by hospCityCountry");

			List<String> distinctCityCountryList = distinctCityCountry
					.toJavaRDD().map(new Function<Row, String>() {
						private static final long serialVersionUID = 1L;

						public String call(Row v1) throws Exception {

							return v1.getString(0);

						}
					}).collect();

			String outputFile0 = "hdfs://spark-01:8020/tmp/ALL_REC.csv";
			String outputFile2 = "hdfs://spark-01:8020/tmp/ALL_PREDICT.csv";

			PrintWriter writer = new PrintWriter(outputFile0, "UTF-8");
			PrintWriter writer2 = new PrintWriter(outputFile2, "UTF-8");

			for (final String d : distinctCityCountryList) {

				System.out.println("distinct city country:::" + d);

				JavaRDD<Rating> trainData = rawUserHospitalData
						.sample(true, 0.5).map(new Function<String, Rating>() {
							private static final long serialVersionUID = 1L;

							public Rating call(String s) throws Exception {
								String[] sarray = s.replaceAll("\\[|\\]", " ")
										.split(",");
								if (d.equals(sarray[8].concat("-" + sarray[10])
										.trim())) {
									int userID = Integer.parseInt(sarray[6]);
									int hospitalID = Integer
											.parseInt(sarray[1]);
									double count = Double
											.parseDouble(sarray[4]);
									// System.out.println(":::userID:"+userID+":::hospitalID:"+hospitalID+":::Rating:"+count);
									return new Rating(userID, hospitalID, count);
								} else {
									return new Rating(-1, -1, -99.0);
								}
							}
						}).filter(new Function<Rating, Boolean>() {
							private static final long serialVersionUID = 1L;

							public Boolean call(Rating v1) throws Exception {
								if (v1.product() == -1)
									return false;
								else
									return true;
							}
						});

				// System.out.println(trainData.collect().toString());

				MatrixFactorizationModel model = ALS.trainImplicit(
									trainData.rdd(), 2, 10, 0.01, 0.01);

				/*************************************************
				 ********** USER ID , Hosp CITY + Hosp Country ***********
				 *************************************************/

				JavaRDD<Tuple2<Integer, String>> rawUserCity = rawUserHospitalData
						.flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
							private static final long serialVersionUID = 1L;

							public Iterable<Tuple2<Integer, String>> call(
									String s) {
								String[] sarray = s.replaceAll("\\[|\\]", " ")
										.split(",");
								List<Tuple2<Integer, String>> returnList = new ArrayList<Tuple2<Integer, String>>();
								if (sarray.length >= 2) {
									try {
										if (Utils.isInteger(sarray[6])
												&& (d.equals(sarray[8].concat(
														"-" + sarray[10])
														.trim()) || d
														.equals(sarray[7]
																.concat("-"
																		+ sarray[9])
																.trim()))) {
											returnList
													.add(new Tuple2<Integer, String>(
															Integer.parseInt(sarray[6]),
															sarray[8]
																	.concat("-"
																			+ sarray[10])
																	.trim()));
											return returnList;
										} else {
											returnList
													.add(new Tuple2<Integer, String>(
															-1, "NA"));
											return returnList;
										}
									} catch (NumberFormatException e) {
										e.printStackTrace();
										returnList
												.add(new Tuple2<Integer, String>(
														-1, "NA"));
										return returnList;
									}
								} else {
									try {
										returnList
												.add(new Tuple2<Integer, String>(
														-1, "NA"));
										return returnList;
									} catch (NumberFormatException e) {
										e.printStackTrace();
										returnList
												.add(new Tuple2<Integer, String>(
														-1, "NA"));
										return returnList;
									}
								}
							}
						});

				JavaPairRDD<Integer, String> rawUserCityRDD = JavaPairRDD
						.fromJavaRDD(rawUserCity);


				// Convert the rawUserCityRDD to elements of class User
				JavaRDD<User> tableUser = rawUserCityRDD
						.map(new Function<Tuple2<Integer, String>, User>() {
							private static final long serialVersionUID = 1L;

							public User call(Tuple2<Integer, String> v1)
									throws Exception {
								User st = new User(v1._1(), v1._2());
								return st;
							}
						});

				DataFrame dfUser = sqlctx
						.createDataFrame(tableUser, User.class);
				dfUser.registerTempTable("User");

				DataFrame distinctUserId = sqlctx
						.sql("select userId FROM User where userId <> -1 AND userCityCountry='"
								+ d + "' group by userId");

				List<Integer> distinctUserIdList = distinctUserId.toJavaRDD()
						.map(new Function<Row, Integer>() {
							private static final long serialVersionUID = 1L;

							public Integer call(Row v1) throws Exception {

								return v1.getInt(0);

							}
						}).collect();

            
				
				// Turn auto commit off (turned on by default)
				con.setAutoCommit(false);
				  // Create insert statement
				 PreparedStatement stmt = con.prepareStatement("INSERT INTO Recommendation VALUES (?)");
				  
				  
				for (Integer u : distinctUserIdList) {

					// System.out.println("u:::"+u);

					try {
						// recommend 5 hospitals
						Rating[] recommendProducts = model.recommendProducts(u,
								5);

						JavaPairRDD<Integer, String> rawHospUserB1 = rawHospUserB
								.getValue();
						List<String> lookupUserHosp = rawHospUserB1.lookup(u);

						int taken = 0;
						for (Rating p : recommendProducts) {

							
							writer2.println(u + ";" + p.productElement(0) + ";"
									+ p.productElement(1) + ";"
									+ p.productElement(2));

							String product = addQuotes(p.product());
							int exist = 0;

							for (String str1 : lookupUserHosp) {

								// / take only 1 hospital to recommend
								if (str1.trim().contains(product)
										|| product.contains("-1"))
									exist = 1;

							}

							if ((exist == 0)
									&& (product.toString() != addQuotes("-1"))
									&& (taken == 0)) {

								String ReommendationID = u + "-" + strDate;

								String outputFile = "hdfs://spark-01:8020/tmp/"
										.concat("" + u + "-").concat(strDate)
										.concat(".json");
								PrintWriter writer1 = new PrintWriter(
										outputFile, "UTF-8");
								writer1.println("{");
								writer1.println(addQuotes("RecUserID") + ":"
										+ addQuotes(ReommendationID) + ",");
								writer1.println(addQuotes("RecHospitalID")
										+ ":" + product.toString().trim());
								writer1.println("}");
								writer1.close();

								writer.println(u + "," + p.product());
								taken = 1;
								// Create a new Clob instance as I'm inserting into a CLOB data type
							      Clob clob = con.createClob();
							      // Store my JSON into the CLOB
							      
							      clob.setString(1, "{RecUserID:"+addQuotes(ReommendationID)+", RecHospitalID: "+product.toString().trim()+"}");
							      // Set clob instance as input parameter for INSERT statement
							      stmt.setClob(1, clob);
							      // Execute the INSERT statement
							      int affectedRows = stmt.executeUpdate();
							      // Free up resource
							      clob.free();
							      // Commit inserted row to the database
							      con.commit();
							}

						}
					} catch (java.util.NoSuchElementException e) {
						System.out
								.println("Warning: No enough elements to recommend");
						// e.printStackTrace();

					}

				}
				trainData.unpersist();
				rawUserCity.unpersist();
				rawUserCityRDD.unpersist();
				tableUser.unpersist();
			}
			writer.close();
			writer2.close();

		} catch (java.lang.NullPointerException e) {
			e.printStackTrace();
		} catch (java.util.NoSuchElementException e) {
			e.printStackTrace();
		}
		sc.close();
		 rs.close();
         statement.close();
         con.close();
	}

	private static String addQuotes(int user) {
		// TODO Auto-generated method stub
		return "\"" + user + "\"";
	}

	static String addQuotes(String in) {
		return "\"" + in + "\"";
	}

	@Override
	public Connection apply() {
		// TODO Auto-generated method stub
		return null;
	}

}
