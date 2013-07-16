package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

class MatrixMult(args : Args) extends Job(args) {
	import Matrix._
	
	val topK = 100
	//read user following graph as matrix
	val matrixA = Tsv( args("graph"), ('user1, 'user2, 'rel) )
    .read
    .toMatrix[Long,Long,Double]('user1, 'user2, 'rel)
	
	//binarize following graph
	val binaryMatrixA = matrixA.binarizeAs[Double]
	
	//read user preference as matrix
	val matrixB = Tsv( args("pref"), ('user, 'item, 'rate) )
    .read
    .toMatrix[Long,Long,Double]('user, 'item, 'rate)
	
	val binaryMatrixB = matrixB.binarizeAs[Double]
	
	// multiply and remove elements already exist in matrixB 
	val mult = (binaryMatrixA * binaryMatrixB)
	val oneMinusB = mult.binarizeAs[Double] - matrixB
	val multWithoutPref = mult.hProd(oneMinusB).topRowElems(topK)
	
	multWithoutPref.write( Tsv( args("output") ) )
	
}
