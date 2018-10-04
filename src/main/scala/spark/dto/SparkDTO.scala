package spark.dto

import java.util.Date

case class Review(movieid: String, reviewid: String, content: String)

case class SteamingRecord(id: String, time: String, recordCount: Long, recordType: String,
                          batchRecordId: String)
