package spark.dto

import java.util.Date

case class Review(movieid: String, reviewid: String, content: String)

case class SteamingRecord(id: String, time: String, recordUpdateCount: Int, recordType: String,
                          batchRecordId: String)
