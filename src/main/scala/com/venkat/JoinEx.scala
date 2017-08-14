package com.venkat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/9/2017.
  */

//pair rdd
object JoinEx extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)

  val rdd1 = sc.parallelize(List("Alice","Bob","Joe")).map(a => (a,1))
  val rdd2 = sc.parallelize((List("John","Alice","Daniel"))).map(a => (a,1))
  val joinCollect = rdd1.join(rdd2).collect()
  joinCollect.foreach(println)

  val ljCollect = rdd1.leftOuterJoin(rdd2).collect()
  ljCollect.foreach(println)

  val rjCollect = rdd1.rightOuterJoin(rdd2).collect()
  rjCollect.foreach(println)

  //combineByKey
  val rdd3 = sc.parallelize(List(
    ("A",3),("A",9),("A",12),("A",0),("A",5),
    ("B",4),("B",10),("B",11),("B",20),("B",25),
    ("C",32),("C",91),("C",122),("C",3),("C",55)),2)

  val rdd4 = rdd3.combineByKey(
    (v:Int) => v.toLong,
    (c:Long,v:Int) => c + v,
    (c1:Long, c2:Long) => c1 + c2
  )

  val comCollect = rdd4.collect()
  comCollect.foreach(println)

  val rdd5 = sc.parallelize(List((1,2),(3,4),(3,6)))
  val count = rdd5.countByKey()
  count.foreach(println)

  val c = rdd5.collectAsMap()
  c.foreach(println)

  val l = rdd5.lookup(3)
  l.foreach(println)
}
