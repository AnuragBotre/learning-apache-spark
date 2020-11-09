package com.trendcore.learning.apache.spark.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, from_json, get_json_object}
import org.apache.spark.sql.types._

object DataFrameComplexCaseClass {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val session = SparkSession.builder().master("local").getOrCreate();



    /*
        Using the schema above, create a Dataset,
        represented as a Scala case type, and generate some JSON data associated with it.
        In all likelihood, this JSON might as well be a stream of device events read off
        a Kafka topic.
        Note that the case class has two fields:
        integer (as a device id) and
        a string (as a JSON string representing device events).
    */

    /*
      Create a Dataset from the above schema
     */

    val context = session.sqlContext;

    /*This line is important. Only this line will give us toDF() method.*/
    import context.implicits._


    case class DeviceData(id: Int, device: String)

    /*
      Input Data
    */
    val eventsDS = Seq(
      (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
      (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
      (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
      (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
      (4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
      (5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
      (6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
      (7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
      (8, """ {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
      (9, """{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }"""),
      (10, """{"device_id": 10, "device_type": "sensor-igauge", "ip": "68.28.91.22", "cca3": "USA", "cn": "United States", "temp": 32, "signal": 26, "battery_level": 7, "c02_level": 886, "timestamp" :1475600518 }"""),
      (11, """{"device_id": 11, "device_type": "sensor-ipad", "ip": "59.144.114.250", "cca3": "IND", "cn": "India", "temp": 46, "signal": 25, "battery_level": 4, "c02_level": 863, "timestamp" :1475600520 }"""),
      (12, """{"device_id": 12, "device_type": "sensor-igauge", "ip": "193.156.90.200", "cca3": "NOR", "cn": "Norway", "temp": 18, "signal": 26, "battery_level": 8, "c02_level": 1220, "timestamp" :1475600522 }"""),
      (13, """{"device_id": 13, "device_type": "sensor-ipad", "ip": "67.185.72.1", "cca3": "USA", "cn": "United States", "temp": 34, "signal": 20, "battery_level": 8, "c02_level": 1504, "timestamp" :1475600524 }"""),
      (14, """{"device_id": 14, "device_type": "sensor-inest", "ip": "68.85.85.106", "cca3": "USA", "cn": "United States", "temp": 39, "signal": 17, "battery_level": 8, "c02_level": 831, "timestamp" :1475600526 }"""),
      (15, """{"device_id": 15, "device_type": "sensor-ipad", "ip": "161.188.212.254", "cca3": "USA", "cn": "United States", "temp": 27, "signal": 26, "battery_level": 5, "c02_level": 1378, "timestamp" :1475600528 }"""),
      (16, """{"device_id": 16, "device_type": "sensor-igauge", "ip": "221.3.128.242", "cca3": "CHN", "cn": "China", "temp": 10, "signal": 24, "battery_level": 6, "c02_level": 1423, "timestamp" :1475600530 }"""),
      (17, """{"device_id": 17, "device_type": "sensor-ipad", "ip": "64.124.180.215", "cca3": "USA", "cn": "United States", "temp": 38, "signal": 17, "battery_level": 9, "c02_level": 1304, "timestamp" :1475600532 }"""),
      (18, """{"device_id": 18, "device_type": "sensor-igauge", "ip": "66.153.162.66", "cca3": "USA", "cn": "United States", "temp": 26, "signal": 10, "battery_level": 0, "c02_level": 902, "timestamp" :1475600534 }"""),
      (19, """{"device_id": 19, "device_type": "sensor-ipad", "ip": "193.200.142.254", "cca3": "AUT", "cn": "Austria", "temp": 32, "signal": 27, "battery_level": 5, "c02_level": 1282, "timestamp" :1475600536 }""")
    ).toDF("id", "device")

    /*
      This will show data present in the dataframe.
    */
    eventsDS.show(false)

    /*
      This will print dataframe schema.
     */

    eventsDS.printSchema();


    /*
      get_json_object api to extract parse json and as column.
    */

    eventsDS
      .select($"id", get_json_object($"device", "$.device_type"),
        get_json_object($"device", "$.ip").as("ip"))
      .show();

    /*
      from_json api.
       This api uses schema to extract individual column names.
       Using this api in select function
       you can extract json values / attributes from Json String into dataframes as columns.
     */

    val jsonSchema = new StructType()
      .add("battery_level", LongType)
      .add("c02_level", LongType)
      .add("cca3", StringType)
      .add("cn", StringType)
      .add("device_id", LongType)
      .add("device_type", StringType)
      .add("signal", LongType)
      .add("ip", StringType)
      .add("temp", LongType)
      .add("timestamp", TimestampType)

    val eventsDF = eventsDS
      .select(from_json($"device", jsonSchema).as("devices"));

    eventsDF.printSchema()

    eventsDF.show(false)

    eventsDF.createOrReplaceTempView("eventsDF");

    session
      .sql("SELECT devices.battery_level,  devices.ip , devices.signal " +
        " FROM eventsDF").show(false);

    eventsDF
      .select($"devices.battery_level", $"devices.ip", $"devices.signal")
      .show(false);


    eventsDF
      .select($"devices.battery_level", $"devices.ip", $"devices.signal")
      .where($"devices.temp" > 10 and $"devices.signal" > 15)
      .show(false);

    eventsDF
      .select($"*")
      .where($"devices.cca3" === "USA")
      .orderBy($"devices.signal".desc, $"devices.temp".desc)
      .show(false);


    eventsDF
      .select($"devices.*")
      .where($"devices.cca3" === "USA")
      .orderBy($"devices.signal".desc, $"devices.temp".desc)
      .show(false);

    /**
     * Nested structure.
     * */

    val schema = new StructType()
      .add("dc_id", StringType)
      .add("source", MapType(
        StringType, new StructType()
          .add("id", StringType)
          .add("description", StringType)
          .add("temp", LongType)
          .add("geo", new StructType()
            .add("lat", DoubleType)
            .add("long", DoubleType)
          )
      )
      );

    val dataFrameWithRespectToAboveSchema = Seq(
      """ {
        |"dc_id": "dc-101",
        |"source": {
        |"sensor-igauge": {"id": 10,
        |"description": "Sensor attached to the container ceilings","temp":35},
        |"sensor-ipad":{"id": 13,
        |"description": "Sensor ipad attached to carbon cylinders","temp":34},
        |"sensor-inest":{"id": 8,
        |"description": "Sensor attached to the factory ceilings","temp":40}
        |}
        |}""".stripMargin).toDS()

    dataFrameWithRespectToAboveSchema.printSchema()

    dataFrameWithRespectToAboveSchema.show(false)

    val complexSchemaDataFrame = session
      .read
      .schema(schema)
      .json(dataFrameWithRespectToAboveSchema)

    complexSchemaDataFrame.printSchema()

    complexSchemaDataFrame.show(false);

    /*
        explode()  API.  This API will give new row for each element in the Map column.
        In this case the map column is source with 3 keys
        namely sensor-igauge , sensor-ipad  , sensor-inest
     */

    val explodedDF = complexSchemaDataFrame
      .select($"dc_id", explode($"source"))

    explodedDF.printSchema()

    explodedDF.show(false)


    val notifydevicesDataSet = explodedDF.select(
      $"dc_id".as("dcId"),
      $"key".as("deviceType"),
      'value.getItem("temp") as 'temp,
      'value.getItem("description") as 'deviceDescription
    ).as[DeviceAlert]

    notifydevicesDataSet.printSchema()

    notifydevicesDataSet.show(false)

    explodedDF.createOrReplaceTempView("explodedDF");

    explodedDF.printSchema()

    session
      .sql("SELECT * "
        + " FROM explodedDF")
      .show(false)

    session
      .sql("SELECT dc_id, key , " +
        " value['temp'] as temp , " +
        " value['description'] as deviceDescription " +
        " FROM explodedDF")
      .show(false)
  }

  case class DeviceAlert(dcId: String, deviceType: String, temp: Long, deviceDescription: String)


}
