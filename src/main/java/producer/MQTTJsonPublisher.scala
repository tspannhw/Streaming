package producer

import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttException, MqttMessage}

import scala.util.Random

object MQTTJsonPublisher extends App {
  def publishToserver(): Unit = {
    println("Hey I am publishing")
    val brokerUrl = s"tcp://localhost:1883"
    val topic = "trx"
    var client: MqttClient = null
    val persistence = new MemoryPersistence
    val transactionCardTypeList = Array("Visa", "MasterCard", "Maestro", "AMEX", "Diners Club", "Revolut")
    val transactionCurrency = Array("USD", "EUR", "CHF")
    val shopName = Array("Tante_Emma", "Aus_der_Region", "Shop_am_Eck", "SihlCity", "BioMarkt")


    try {
      client = new MqttClient(brokerUrl, MqttClient.generateClientId, persistence)
      client.connect()
      val msgTopic = client.getTopic(topic)

      while (true) {
        val jsonmessage =
          s"""{
             | "timestamp": "${System.currentTimeMillis()}",
             | "shop_id": "${Random.nextInt(5)}",
             | "shop_name": "${Random.shuffle(shopName.toList).head}",
             | "cc_type": "${Random.shuffle(transactionCardTypeList.toList).head}",
             | "cc_id": "51${10 + Random.nextInt(89)}-${1000 + Random.nextInt(8999)}-${1000 + Random.nextInt(8999)}-${1000 + Random.nextInt(8999)}",
             | "amount_orig": ${Math.round((Math.random * 17000) + 100) / 100.0},
             | "fx": "${Random.shuffle(transactionCurrency.toList).head}",
             | "fx_account": "${Random.shuffle(transactionCurrency.toList).head}"
             |}""".stripMargin

        val message = new MqttMessage(jsonmessage.getBytes("utf-8"))
        msgTopic.publish(message)
        println(s"${message.toString} ")
        Thread.sleep((Math.random * 5000).toLong)
      }
    }
    catch {
      case exception: MqttException => println(s"ExceptionOccured:$exception ")
    }
    finally {
      client.disconnect()
    }
  }

  publishToserver()

}