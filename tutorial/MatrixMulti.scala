package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

class MatrixMult(args : Args) extends Job(args) {
	import Matrix._
	
	val topK = 100
	
	val matrixA = Tsv( args("graph"), ('user1, 'user2, 'rel) )
    .read
    .toMatrix[Long,Long,Double]('user1, 'user2, 'rel)
	
	val binaryMatrixA = matrixA.binarizeAs[Int]
	
	val matrixB = Tsv( args("preference"), ('user, 'item, 'rate) )
    .read
    .toMatrix[Long,Long,Double]('user, 'item, 'rate)
	
	val binaryMatrixB = matrixB.binarizeAs[Int]
	
	val mult = (binaryMatrixA * binaryMatrixB).topRowElems(topK)
	
	mult.write( Tsv( args("output") ) )
	
}
