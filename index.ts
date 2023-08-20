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
let BROKER_HOST = process.env.BROKER_HOST
let BROKER_PORT = process.env.BROKER_PORT
let USE_BLUETOOTH = process.env.USE_BLUETOOTH
let USE_LAN = process.env.USE_LAN

const { Kafka, CompressionTypes, logLevel } = require('kafkajs')

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${BROKER_HOST}:${BROKER_PORT}`],
  clientId: 'ditto-producer',
})

async function main() {
  await init()

  const config = new TransportConfig()
  config.peerToPeer.bluetoothLE.isEnabled = USE_BLUETOOTH
  config.peerToPeer.lan.isEnabled = USE_LAN
  config.peerToPeer.awdl.isEnabled = false

  ditto = new Ditto({ type: 'onlinePlayground',
	  appID: APP_ID,
	  token: APP_TOKEN,
    enableDittoCloudSync: false,
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
  
  rawSubscription = ditto.store.collection(RAW_COLLECTION_NAME).find("synced == false").subscribe()
  let rawDocuments: Document[] =[]

  rawLiveQuery = ditto.store
    .collection(RAW_COLLECTION_NAME)
    .find("synced == false")
    .observeLocalWithNextSignal(async (docs, event, signalNext) => {
      for (let i = 0; i < docs.length; i++) {
        const rawDoc = docs[i]
        // Send to RedPanda topic
       console.log("Sending to RedPanda - num of docs: ", docs.length)
       await producer.send({
         topic: RAW_TOPIC_NAME,
         messages: [
          { key: rawDoc.value.quartetId + "." + rawDoc.value.nodeId, value: JSON.stringify(rawDoc.value) }
         ],
         compression: CompressionTypes.GZIP,
       })
        await ditto.store.collection(RAW_COLLECTION_NAME).findByID(rawDoc.id).update((mutableDoc) => {
          mutableDoc.at('synced').set(true)
        }) 
        // Set synced, and evict
      }
    //  await ditto.store.collection(RAW_COLLECTION_NAME).find("synced == true").evict()
    //  console.log("XXX EVICTED!!!")
      signalNext()
    })

  productSubscription = ditto.store.collection(PRODUCT_COLLECTION_NAME).find("synced == false").subscribe()
  productLiveQuery = ditto.store
    .collection(PRODUCT_COLLECTION_NAME)
    .find("synced == false")
    .observeLocalWithNextSignal(async (docs, event, signalNext) => {
      for (let i = 0; i < docs.length; i++) {
        const productDoc = docs[i]
        console.log("PRODUCT DOC to RedPanda: ", productDoc)
        producer.send({
          topic: PRODUCT_TOPIC_NAME,
          messages: [
            { value: JSON.stringify(productDoc.value) }
          ],
        })
        await ditto.store.collection(PRODUCT_COLLECTION_NAME).findByID(productDoc.id).update((mutableDoc) => {
          mutableDoc.at('synced').set(true)
        }) 
        // Set synced, and evict
      }
      ditto.store.collection(PRODUCT_COLLECTION_NAME).find("synced == true").evict()
      signalNext()
    })

  const presenceObserver = ditto.presence.observe((graph) => {
    if (graph.remotePeers.length != 0) {
      graph.remotePeers.forEach((peer) => {
        console.log("peer connection: ", peer.deviceName, peer.connections[0].connectionType)
      })
    }
  })

}

main()
