import scala.collection.immutable.Map.from
import scala.io.StdIn._
import scala.util.control.Breaks
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object test_login extends App{

  var user_list = Map(("douglas", "mypass"),("bob", "bobpass"))
  var admin_list = Map(("douglas", "admin"),("bob", "basic"))

  def login():Unit = {
    val enter_username = readLine("Enter username: ")
    val enter_password = readLine("Enter password: ")
    val user_auth = admin_list(enter_username)
    if (user_list.contains(enter_username) & enter_password == user_list(enter_username)) {
      println("\nLogged in.\n")

      var loggedin_command = ""
      loop.breakable {
        while (loggedin_command != "9") {
          try {
            loggedin_command = readLine("\nApp Menu\n[1]Create user\n" +
              "[2]Read user list\n[3]Update password\n[4]Delete user\n[5]Analytics\n[9]Logout\nEnter a command: ")
            if (loggedin_command == "1") {
              if (user_auth != "admin") {
                println("\nYou are not an admin.\nPermission denied.")
              }
              else {
                add_user()
              }
            }
            else if (loggedin_command=="2"){
              read_user()
            }
            else if (loggedin_command=="3"){
              if (user_auth != "admin") {
                println("\nYou are not an admin.\nPermission denied.")
              }
              else {
                change_pass()
              }
            }
            else if (loggedin_command=="4"){
              if (user_auth != "admin") {
                println("\nYou are not an admin.\nPermission denied.")
              }
              else {
                del_user()
              }
            }
            else if (loggedin_command=="5"){
              println("Analytics Menu")
              analytics()
            }
            else if (loggedin_command == "9") {
              println("\nLogging out!\n")
              loop.break
            }
            else {
              println(s"$loggedin_command, is not a valid command.")
            }
          }
          catch {case a: Exception => println("Incorrect input")}

        }//while end

      }//loop end

    }
    else {
      println("Incorrect login")
    }//if else end
  }//end login


  def analytics(): Unit ={

    def load_database(): Unit = {
      try {
        println("\nCreating Tables...\n")

        //allows partitions
        spark.sql("Set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("DROP TABLE IF EXISTS wine_partition")
        //empty partitioned tables
        spark.sql("create table wine_partition(fixed_acidity DOUBLE, volatile_acidity DOUBLE, citric_acid DOUBLE," +
          "residual_sugar DOUBLE, chlorides DOUBLE, free_sulfur_dioxide DOUBLE, total_sulfur_dioxide DOUBLE, density DOUBLE," +
          "ph DOUBLE, sulphates DOUBLE, alcohol DOUBLE, id INT) partitioned by (quality Int) " +
          "row format delimited fields terminated by ',' stored as textfile")



        //populated original table
        spark.sql("DROP table IF EXISTS wine_quality")
        spark.sql("create table wine_quality(fixed_acidity DOUBLE, volatile_acidity DOUBLE, citric_acid DOUBLE," +
          "residual_sugar DOUBLE, chlorides DOUBLE, free_sulfur_dioxide DOUBLE, total_sulfur_dioxide DOUBLE, density DOUBLE," +
          "ph DOUBLE, sulphates DOUBLE, alcohol DOUBLE, quality Int, id INT) row format delimited fields terminated by ',' stored as textfile")
        spark.sql("LOAD DATA LOCAL INPATH 'input3/WineQT.csv' INTO TABLE wine_quality")


        //insert original table data into partitioned tables
        spark.sql("insert overwrite table wine_partition partition(quality) select fixed_acidity,volatile_acidity,citric_acid," +
          "residual_sugar,chlorides,free_sulfur_dioxide,total_sulfur_dioxide,density,ph,sulphates,alcohol,id, quality from wine_quality")

        //analytics
        spark.sql("SELECT Count(*) AS ROWCOUNT FROM wine_quality").show()
        spark.sql("select * from wine_quality").show()
        spark.sql("SHOW PARTITIONS wine_partition").show()

        //bucketed table
        spark.sql("DROP TABLE IF EXISTS bucketed_wine")
        spark.sqlContext.table("wine_partition").write.bucketBy(10, "id").saveAsTable("bucketed_wine")
        //spark.close()
      }//end try
      catch {case a: Exception => println("Tables already created")}
    }//end of load_database()

    def read_partitions(): Unit ={
      try {
        println("reading partitions...")
        spark.sql("show partitions wine_partition").show()
      }
      catch {case a: Exception => println("\nNo partitions found\n")}
    }

    def delete_tables(): Unit ={
      spark.sql("DROP TABLE IF EXISTS bucketed_wine")
      spark.sql("DROP TABLE IF EXISTS wine_partition")
      spark.sql("DROP table IF EXISTS wine_quality")
      println("\nTables deleted\n")
    }

    var analytics_command = ""
    loop.breakable {
      while (analytics_command != "9"){
        try {
          analytics_command = readLine("\nAnalytics Menu\n[1]Read Partitions\n[2]Create tables\n[3]Delete tables\n[4-8],[0]Run queries\n[9]Exit analytics\nEnter a command: ")
          if (analytics_command=="1"){
            read_partitions
          }
          else if (analytics_command=="2"){
            load_database
          }
          else if (analytics_command=="3"){
            delete_tables
          }
          else if (analytics_command=="4"){
            println("Running query: \n")
            println("Show top 10 results by highest quality")
            val query1 = "select * from wine_quality order by quality desc limit 10"
            println(query1)
            spark.sql("select * from wine_quality limit 10").show()
          }
          else if (analytics_command=="5"){
            println("Running query: \n")
            println("Find average of each column, grouped by quality")
            val query2 = "select " +
              "avg(fixed_acidity)," +
              "avg(volatile_acidity)," +
              "avg(citric_acid)," +
              "avg(residual_sugar)," +
              "avg(chlorides)," +
              "avg(free_sulfur_dioxide)," +
              "avg(total_sulfur_dioxide)," +
              "avg(density)," +
              "avg(ph)," +
              "avg(sulphates)," +
              "avg(alcohol)," +
              "quality " +
              "from wine_quality group by quality order by quality desc limit 10"
            println(query2)
            spark.sql(query2).show
          }
          else if (analytics_command=="6"){
            println("Running query: \n")
            println("Find maximum of each column, grouped by quality")
            val query3 = "select " +
              "max(fixed_acidity)," +
              "max(volatile_acidity)," +
              "max(citric_acid)," +
              "max(residual_sugar)," +
              "max(chlorides)," +
              "max(free_sulfur_dioxide)," +
              "max(total_sulfur_dioxide)," +
              "max(density)," +
              "max(ph)," +
              "max(sulphates)," +
              "max(alcohol)," +
              "quality " +
              "from wine_quality group by quality order by quality desc limit 10"
            println(query3)
            spark.sql(query3).show()
          }
          else if (analytics_command=="7"){
            println("Running query: \n")
            println("Find minimum of each column, grouped by quality")
            val query4 ="select " +
              "min(fixed_acidity)," +
              "min(volatile_acidity)," +
              "min(citric_acid)," +
              "min(residual_sugar)," +
              "min(chlorides)," +
              "min(free_sulfur_dioxide)," +
              "min(total_sulfur_dioxide)," +
              "min(density)," +
              "min(ph)," +
              "min(sulphates)," +
              "min(alcohol)," +
              "quality " +
              "from wine_quality group by quality order by quality desc limit 10"
            println(query4)
            spark.sql(query4).show()
          }
          else if (analytics_command=="8"){
            println("Running query: \n")
            println("Find minimum, average, and maximum alcohol grouped by quality")
            val query5 = "select min(alcohol), avg(alcohol), max(alcohol), quality from wine_quality group by quality order by quality desc limit 10"
            println(query5)
            spark.sql(query5).show()
          }
          else if (analytics_command=="0"){
            println("Running query: \n")
            println("Find distinct ph levels")
            val query6 = "select distinct ph from wine_quality order by ph desc limit 10"
            println(query6)
            spark.sql(query6).show()
          }
          else if (analytics_command=="9"){
            println("Exiting Analytics!")
            loop.break
          }
          else {println(s"$analytics_command, is not a valid command.")}
        }
        catch {
          case a: Exception => println("Incorrect input")
        }
      }//end while
    }//end loop break
  }//end of analytics()


  //create user
  def add_user():Unit ={
    try {
      val username = readLine("Enter username: ")
      val password = readLine("Enter password: ")
      user_list = user_list ++ Map((username, password))
      val account_type = readLine("Is this account an [1]admin or [2]basic user? ")
      if (account_type == "1") {
        admin_list = admin_list ++ Map((username, "admin"))
        println(s"An admin account has been made for $username!\n")
      }
      else if (account_type == "2") {
        admin_list = admin_list ++ Map((username, "basic"))
        println(s"A basic account has been made for $username!\n")
      }
      else {println(s"$account_type, is not a valid command.")}
    }
    catch {case a: Exception => println("Incorrect input")}
  }//add_user end

  //read user names
  def read_user(): Unit ={
    admin_list.keys.foreach(i=>println(i+": "+admin_list(i)))
  }


  //update user password
  def change_pass():Unit={
    try {
      val username = readLine("Enter username: ")
      if (user_list.contains(username)) {
        val new_password = readLine("Enter new password: ")
        user_list = user_list - username
        user_list = user_list ++ Map((username, new_password))
      }
      else {
        println(username + " is not a valid user.")
      }
    }
    catch {case a: Exception => println("Incorrect input")}
  }//change_pass end

  //delete user
  def del_user(): Unit ={
    try {
      val username = readLine("Enter username: ")
      if (user_list.contains(username)) {
        user_list = user_list - username
        admin_list = admin_list - username
      }
    }
    catch {case a: Exception => println("Incorrect input")}
  }//del_user end



  //main app loop
  //run once
  //tables under C:\Users\Douglas\Desktop\test_spark\spark-warehouse
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  //spark is class: class org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  println("\ncreated spark session\n")
  spark.sparkContext.setLogLevel("ERROR")


    var command_entered = ""
    val loop = new Breaks
    loop.breakable {
      while (command_entered != "9") {
        try {
          command_entered = readLine("\nWelcome to the App! Please Sign In!\n[1]Login\n[9]Exit app\nEnter a command: ")
          if (command_entered == "1") {
            login()
          }
          else if (command_entered == "9") {
            spark.close()
            println("\nExiting App. Goodbye!")
            loop.break
          }
          else {
            println(s"$command_entered, is not a valid command.")
          }
        }
        catch {
          case a: Exception => println("Incorrect input")
        }
      } //end while
    } //end loop



}//end App
