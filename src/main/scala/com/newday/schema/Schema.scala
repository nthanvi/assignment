package com.newday.schema

trait Schema;

case class Movie(movieId: Short, title: String, genre: String) extends Schema

case class Rating(userId: Short, movieId: Short, rating: Short, time: Long) extends Schema

case class MovieRating(movieId: Short, title: String, genre: String, minimumRating: Int, maximumRating: Int, averageRating: Double) extends Schema

case class UsersFavourite(userId: Short, movieId:Short, title: String, rank:Int) extends Schema