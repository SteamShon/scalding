package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

class BuildEdgeList(args : Args) extends Job(args) {
	import Matrix._
	val prefix = args.optional("prefix")
	val topK = args.getOrElse("topK", "1000").toInt

	val graph = Tsv( args("graph"), ('user, 'item, 'rate))
	.groupBy('user) {
		_.sortWithTake( ('item, 'rate) -> 'topKs, topK) {
			(x: (Long, Double), y: (Long, Double)) => x._2 > y._2
		}
	}
	.mapTo(('user, 'topKs) -> ('user, 'topList)) {
		fields : (String, List[(Long, Double)]) => 
		val (user, topKs) = fields
		val tmp = topKs.map(t => t._1 + ":" + t._2).mkString(",")
		prefix match {
			case Some(value) => (value + ":" + user, tmp)
			case _ => (user, tmp)
		}
	}
	.write( Tsv (args("output") ) )
}
