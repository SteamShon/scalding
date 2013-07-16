package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

class JoinJob(args : Args) extends Job(args) {
	val source = Tsv ( args.getOrElse("source", "source.txt"), ('user, 'item, 'rate) )
	val reference = Tsv ( args.getOrElse("ref", "ref.txt"), ('user2, 'item2, 'rate2) )
	
	val joined = source.joinWithLarger(('user, 'item) -> ('user2, 'item2), reference)
	.project(('user, 'item, 'rate))
	.write( Tsv ( args("output") ))
}
