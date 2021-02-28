package com.newday.log

import org.slf4j.{Logger, LoggerFactory}

trait Logging {
  lazy val logger = LoggerFactory.getLogger(getClass)
  implicit def logging2Logger(anything: Logging): Logger = anything.logger
}