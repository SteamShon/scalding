package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

class LabelPropagation(args : Args) extends Job(args) {
	import Matrix._
	
	val topK = 100
	//read user following graph as matrix
	val NN = 100
	val THRESHOLD = 0.0001
	val iterations = 3
	
	val matrixA = Tsv( args("graph"), ('user1, 'user2, 'rel) )
    .read
    .toMatrix[Long,Long,Double]('user1, 'user2, 'rel).rowL1Normalize.topRowElems(NN)
	
	
	//read user preference as matrix
	val matrixB = Tsv( args("pref"), ('user, 'item, 'rate) )
    .read
    .toMatrix[Long,Long,Double]('user, 'item, 'rate).rowL1Normalize
	
	
	//binarize following graph
	val binaryMatrixA = matrixA.binarizeAs[Double]
	val binaryMatrixB = matrixB.binarizeAs[Double]
	
	// multiply and remove elements already exist in matrixB 
	val scores = propagate(matrixA, matrixB, iterations)
	scores.hProd(scores.binarizeAs[Double] - matrixB).topRowElems(topK)
	.write( Tsv ( args("output") ))
	
	
	def propagate(weightMatrix: Matrix[Long,Long,Double], 
					prevMatrix: Matrix[Long,Long,Double], 
					numIterations: Int) : Matrix[Long,Long,Double] = {
        if (numIterations <= 0) {
			prevMatrix
        } else {
			propagate(weightMatrix, (weightMatrix * prevMatrix).filterValues( _ > THRESHOLD ), numIterations - 1)
        }
	}
}
