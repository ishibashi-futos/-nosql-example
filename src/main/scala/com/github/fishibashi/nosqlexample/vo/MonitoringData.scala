package com.github.fishibashi.nosqlexample.vo

import scala.collection.immutable.Map

class MonitoringData(
                      val refVo: DiagnosReferenceVo,
                      val pointVoList: Seq[DiagnosPointRefVo]
                    )

class DiagnosReferenceVo(val taskId: String,
                         val startTime: String,
                         val endTime: String,
                         val agentId: String,
                         val totalTime: BigInt,
                         val systemId: String,
                         val functionId: String,
                         val screenId: String,
                         val actionId: String)

class DiagnosPointRefVo(val referenceId: String,
                        val parentReferenceId: String,
                        val pointCount: Int,
                        val className: String,
                        val methodName: String,
                        val dataTable: DataTable,
                        val taskId: String,
                        val startTime: String,
                        val endTime: String,
                        val isSuccess: Boolean,
                        val totalTime: Int,
                       )

class DataTable(val args: Map[String, Object],
                val result: Map[String, Object],
                val stackTrace: String)
