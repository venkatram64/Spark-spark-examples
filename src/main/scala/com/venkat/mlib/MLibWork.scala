package com.venkat.mlib

import org.apache.spark.mllib.linalg.{Vector,Vectors}

/**
  * Created by Venkatram on 7/11/2017.
  */
object MLibWork extends App{

  val div:Vector = Vectors.dense(1.0,0.0,3.0)
  val denseVecotr2 = Vectors.dense(Array(1.0,2.0,3.0))

  val sparseVec1 = Vectors.sparse(4,Array(0,2),Array(1.0,2.0))
  val sv2:Vector = Vectors.sparse(3, Seq((0,1.0),(2,3.0)))

}
