package com.github.fishibashi.nosqlexample.db

import com.github.fishibashi.nosqlexample.vo.MonitoringData

trait Dao {
  def save(data: MonitoringData): Unit
  def saveAll(data: Seq[MonitoringData])
  def findByTaskId(): MonitoringData
  def findByStartTime(startTime: String, limit: Int, offset: Int)
  def delete(taskId: String)
  def deleteFromRange(startTime: String, endTime: String)
  def update(data: MonitoringData): Unit
}
