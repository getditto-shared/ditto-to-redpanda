import { init, Ditto, Document, TransportConfig } from '@dittolive/ditto'
require('dotenv').config()

let ditto

let rawSubscription
let rawLiveQuery
let rawDocuments: Document[] = []

let productSubscription
let productLiveQuery
let productDocuments: Document[] = []

let APP_ID = process.env.APP_ID
//let OFFLINE_TOKEN = process.env.OFFLINE_TOKEN
//let SHARED_KEY = process.env.SHARED_KEY
let APP_TOKEN = process.env.APP_TOKEN
let RAW_COLLECTION_NAME = process.env.RAW_COLLECTION_NAME
let PRODUCT_COLLECTION_NAME = process.env.PRODUCT_COLLECTION_NAME
let RAW_TOPIC_NAME = process.env.RAW_TOPIC_NAME
let PRODUCT_TOPIC_NAME = process.env.PRODUCT_TOPIC_NAME

const { Kafka, CompressionTypes, logLevel } = require('kafkajs')
const ip = require('ip')
const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: [`${host}:9092`],
  clientId: 'ditto-producer',
})

async function main() {
  await init()

  const config = new TransportConfig()
  config.peerToPeer.bluetoothLE.isEnabled = true
  config.peerToPeer.lan.isEnabled = false
  config.peerToPeer.awdl.isEnabled = false

  ditto = new Ditto({ type: 'onlinePlayground',
	  appID: APP_ID,
	  token: APP_TOKEN
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
  
  rawSubscription = ditto.store.collection(RAW_COLLECTION_NAME).find("synced == false").subscribe()
  rawLiveQuery = ditto.store
    .collection(RAW_COLLECTION_NAME)
    .find("synced == false")
    .observeLocal((docs, event) => {
      rawDocuments = docs
      let rawDoc
      rawDocuments.map((rawDoc) => {
        // Send to RedPanda topic
        producer.send({
          topic: RAW_TOPIC_NAME,
          messages: [
            { payload: rawDoc.value }
          ],
        })
        rawDoc.value.synced = true
        // Set synced, and evict
        ditto.store.collection(RAW_COLLECTION_NAME).upsert(rawDoc)
        ditto.store.collection(RAW_COLLECTION_NAME).evict(rawDoc)
      })
    })

  productSubscription = ditto.store.collection(PRODUCT_COLLECTION_NAME).find("synced == false").subscribe()
  productLiveQuery = ditto.store
    .collection(PRODUCT_COLLECTION_NAME)
    .find("synced == false")
    .observeLocal((docs, event) => {
      productDocuments = docs
      let productDoc
      productDocuments.map((rawDoc) => {
        producer.send({
          topic: PRODUCT_TOPIC_NAME,
          messages: [
            { payload: productDoc.value }
          ],
        })
        productDoc.value.synced = true
        // Set synced, and evict
        ditto.store.collection(PRODUCT_COLLECTION_NAME).upsert(productDoc)
        ditto.store.collection(PRODUCT_COLLECTION_NAME).evict(productDoc)
      })
    })

  const presenceObserver = ditto.presence.observe((graph) => {
    console.log("local peer connections: ", graph.localPeer.connections)
    if (graph.localPeer.connections.length != 0) {
    }
  })

}

main()
