package com.example

case class User(
                 id: Option[Long],
                 country: Option[String],
                 points: Option[Long],
                 title: Option[String],
                 variety: Option[String],
                 winery: Option[String]
               )