package com.qtech.etl.utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import play.api.libs.mailer._

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/05/29 08:38:50
 * desc   :  Scala发送邮件类
 */

// Scala没有静态方法或静态字段，可以使用object这个语法达到相同的目的，对象定义了某个类的单个实例
object Email4S {

  def createMailer(host: String, port: Int, user: String, password: String, timeout: Int = 100000, connectionTimeOut: Int = 100000): SMTPMailer = {

    val configuration = new SMTPConfiguration(
      host, port, false, false, false, Option(user), Option(password), false, timeout = Option(timeout), connectionTimeout = Option(connectionTimeOut),
      ConfigFactory.empty(), false)

    val mailer: SMTPMailer = new SMTPMailer(configuration)
    mailer
  }

  def createEmail(
                   subject: String,
                   from: String,
                   to: Seq[String],
                   bodyText: String = "ok",
                   bodyHtml: String = "",
                   charset: String = "gbk",
                   attachments: Seq[Attachment] = Seq.empty
                 ): Email = {
    val email: Email = Email(subject, from, to, bodyText = Option[String](bodyText), charset = Option[String](charset), bodyHtml = Option[String](bodyHtml), attachments = attachments)
    email
  }

  def createAttachment(attachmentName: String, rawData: RDD[(String, String, String, String, String, String)], mimetype: String = "text/csv"): Attachment = {
    val data: Array[Byte] = rawData.collect().mkString("\n").getBytes("gbk")
    val attachment: AttachmentData = AttachmentData(attachmentName, data, mimetype)
    attachment
  }
}
