package spark.dto

import java.util.Date

case class Doulist(id: Long, movieid: String, doulistUrl: String, doulistName: String, doulistIntr: String,
                   userName: String, userUrl: String, collectNum: Integer, recommendNum: Integer, movieNum: Integer,
                   doulistCratedDate: Date, doulistUpdatedDate: Date, createdTime: Date, updatedTime: Date)


case class DoulistMovieDetail(id: Long, movieid: String, doulistUrl: String, createdTime: Date, updatedTime: Date)

case class FilmCritics(id: Long, movieid: String, filmCriticsUrl: String, title: String, userName: String,
                       userUrl: String, commentRate: Double, commentTime: Date, uselessNum: Integer,
                       usefulNum: Integer, likeNum: Integer, recommendNum: Integer,
                       review: String, createdTime: Date, updatedTime: Date)

case class MovieBaseInfo(id: Long, movieid: String, movieName: String, viewDate: Date, personalRate: Integer,
                         personalTags: String, intro: String, isViewed: String,
                         createdTime: Date, updatedTime: Date)

case class MovieDetail(id: Long, movieid: String, movieUrl: String, movieName: String, director: String,
                       writers: String, stars: String, genres: String, country: String, officialSite: String, language: String,
                       releaseDate: String, alsoKnown_as: String, runtime: String, IMDbUrl: String, doubanRate: Double, rateNum: Integer,
                       star5: String, star4: String, star3: String, star2: String, star1: String,
                       comparison1: String, comparison2: String, tags: String, storyline: String,
                       alsoLike1Name: String, alsoLike1Url: String, alsoLike2Name: String, alsoLike2Url: String,
                       alsoLike3Name: String, alsoLike3Url: String, alsoLike4Name: String, alsoLike4Url: String,
                       alsoLike5Name: String, alsoLike5Url: String, alsoLike6Name: String, alsoLike6Url: String,
                       alsoLike7Name: String, alsoLike7Url: String, alsoLike8Name: String, alsoLike8Url: String,
                       alsoLike9Name: String, alsoLike9Url: String, alsoLike10Name: String, alsoLike10Url: String,
                       essayCollectUrl: String, filmCriticsUrl: String, doulistsUrl: String,
                       viewedNum: Integer, wantToViewNum: Integer, imageUrl: String,
                       createdTime: Date, updatedTime: Date)

case class MovieEssay(id: Long, movieid: String, userName: String, userUrl: String, comment: String,
                      commentRate: Double, commentTime: Date, createdTime: Date, updatedTime: Date)



case class Review(movieid:String,reviewid:String,content:String)


