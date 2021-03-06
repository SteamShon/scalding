package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

class BuildChatGraph(args : Args) extends Job(args) {
	import Matrix._
	
	val graph = Tsv( args("graph"), ('chat_id, 'user_id, 'msg_cnt))
	.mapTo(('chat_id, 'user_id, 'msg_cnt) -> ('chat_id, 'user_id_msg_cnt)) {
		fields : (Long, Long, Int) => 
		val (chat_id, user_id, msg_cnt) = fields
		(chat_id, user_id + ":" + msg_cnt)
	}
	.write( Tsv( args("output")))
	
}
