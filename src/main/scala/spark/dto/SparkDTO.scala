package spark.dto

import java.sql.Timestamp

case class Review(movieid: String, reviewid: String, content: String)

case class SteamingRecord(id: String, startTime: Timestamp, endTime:Timestamp,recordCount: Long, recordType: String,
                          batchRecordId: String, createdTime: Timestamp, updatedTime: Timestamp)
