import { init, Ditto, Document, Logger, TransportConfig } from '@dittolive/ditto'
require('dotenv').config()

Logger.minimumLogLevel = 'Info'

let ditto

let rawSubscription
let rawLiveQuery

let productSubscription
let productLiveQuery

let presence

let APP_ID = process.env.APP_ID
//let OFFLINE_TOKEN = process.env.OFFLINE_TOKEN
//let SHARED_KEY = process.env.SHARED_KEY
let APP_TOKEN = process.env.APP_TOKEN
let RAW_COLLECTION_NAME = process.env.RAW_COLLECTION_NAME
let RAW_TOPIC_NAME = process.env.RAW_TOPIC_NAME
let PRODUCT_COLLECTION_NAME = process.env.PRODUCT_COLLECTION_NAME
let PRODUCT_TOPIC_NAME = process.env.PRODUCT_TOPIC_NAME
let BROKER_HOST = process.env.BROKER_HOST
let BROKER_PORT = process.env.BROKER_PORT
let USE_BLUETOOTH = (process.env.USE_BLUETOOTH === "True")
let USE_LAN = (process.env.USE_LAN === "True")
let USE_CLOUD = (process.env.USE_CLOUD == "True")

const { Kafka, CompressionTypes, logLevel } = require('kafkajs')

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${BROKER_HOST}:${BROKER_PORT}`],
  clientId: 'ditto-producer',
})

async function main() {
  await init()
  Logger.info(`Starting up Base node - broker: ${BROKER_HOST}:${BROKER_PORT}, BLE: ${USE_BLUETOOTH}, LAN: ${USE_LAN}`)
  const config = new TransportConfig()
  config.peerToPeer.bluetoothLE.isEnabled = USE_BLUETOOTH
  config.peerToPeer.lan.isEnabled = USE_LAN
  config.peerToPeer.awdl.isEnabled = false

  ditto = new Ditto({ type: 'onlinePlayground',
	  appID: APP_ID,
	  token: APP_TOKEN,
    enableDittoCloudSync: USE_CLOUD,
  })

  const transportConditionsObserver = ditto.observeTransportConditions((condition, source) => {
     if (condition === 'BLEDisabled') {
       console.log('BLE disabled')
     } else if (condition === 'NoBLECentralPermission') {
       console.log('Permission missing for BLE')
     } else if (condition === 'NoBLEPeripheralPermission') {
       console.log('Permissions missing for BLE')
     }
   })

  ditto.setTransportConfig(config)

  const producer = kafka.producer()
  await producer.connect()
  ditto.startSync()
 // AND all data < current timestamp
  
  rawSubscription = ditto.store.collection(RAW_COLLECTION_NAME).find("state == 'collected'").subscribe()

  rawLiveQuery = ditto.store
    .collection(RAW_COLLECTION_NAME)
    .find("state == 'collected'")
    .observeLocalWithNextSignal(async (docs, event, signalNext) => {
      Logger.info(`Sending to RedPanda - ${docs.length} docs`)
      for (let i = 0; i < docs.length; i++) {
        const rawDoc = docs[i]
        // Send to RedPanda topic
        await producer.send({
          topic: RAW_TOPIC_NAME,
          messages: [
            { key: rawDoc.value.quartetId + "." + rawDoc.value.nodeId, value: JSON.stringify(rawDoc.value) }
          ],
          compression: CompressionTypes.GZIP,
        })
        await ditto.store.collection(RAW_COLLECTION_NAME).findByID(rawDoc.id).update((mutableDoc) => {
          mutableDoc.at("state").set("delivered")
        }) 
      }
      await ditto.store.collection(RAW_COLLECTION_NAME).find("state == 'delivered'").evict()
      console.log("XXX EVICTED!!!")
      signalNext()
    })

  productSubscription = ditto.store.collection(PRODUCT_COLLECTION_NAME).find("state == 'collected'").subscribe()
  productLiveQuery = ditto.store
    .collection(PRODUCT_COLLECTION_NAME)
    .find("state == 'collected'")
    .observeLocalWithNextSignal(async (docs, event, signalNext) => {
      for (let i = 0; i < docs.length; i++) {
        const productDoc = docs[i]
        producer.send({
          topic: PRODUCT_TOPIC_NAME,
          messages: [
            { value: JSON.stringify(productDoc.value) }
          ],
        })
        await ditto.store.collection(PRODUCT_COLLECTION_NAME).findByID(productDoc.id).update((mutableDoc) => {
          mutableDoc.at("state").set("delivered")
        }) 
        // Set synced, and evict
      }
      // The eviction process is causing sync looping issues.  Need to determine optimal configuration
      // Working with Ditto engineering to resolve - Kit
      // ditto.store.collection(PRODUCT_COLLECTION_NAME).find("state == 'delivered'").evict()
      signalNext()
    })

  presence = ditto.presence.observe((graph) => {
    if (graph.remotePeers.length != 0) {
      graph.remotePeers.forEach((peer) => {
        Logger.info(`peer connection: ${peer.deviceName}, ${peer.connections[0].connectionType}`)
      })
    }
  })

}

main()
