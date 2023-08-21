import { init, Ditto, Document, Logger, TransportConfig } from '@dittolive/ditto'
require('dotenv').config()

Logger.minimumLogLevel = 'Info'

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
let RAW_TOPIC_NAME = process.env.RAW_TOPIC_NAME
let RAW_QUERY_KEY = process.env.RAW_QUERY_KEY
let RAW_QUERY_VALUE = process.env.RAW_QUERY_VALUE
let PRODUCT_COLLECTION_NAME = process.env.PRODUCT_COLLECTION_NAME
let PRODUCT_TOPIC_NAME = process.env.PRODUCT_TOPIC_NAME
let PRODUCT_QUERY_KEY = process.env.PRODUCT_QUERY_KEY
let PRODUCT_QUERY_VALUE = process.env.PRODUCT_QUERY_VALUE
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
  Logger.info(`Starting up - broker: ${BROKER_HOST}:${BROKER_PORT}, BLE: ${USE_BLUETOOTH}, LAN: ${USE_LAN}`)
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
  
  rawSubscription = ditto.store.collection(RAW_COLLECTION_NAME).find(`${RAW_QUERY_KEY} == ${RAW_QUERY_VALUE}`).subscribe()
  let rawDocuments: Document[] =[]

  rawLiveQuery = ditto.store
    .collection(RAW_COLLECTION_NAME)
    .find(`${RAW_QUERY_KEY} == ${RAW_QUERY_VALUE}`)
    .observeLocalWithNextSignal(async (docs, event, signalNext) => {
      for (let i = 0; i < docs.length; i++) {
        const rawDoc = docs[i]
        // Send to RedPanda topic
       Logger.info("Sending to RedPanda - num of docs: ", docs.length)
       await producer.send({
         topic: RAW_TOPIC_NAME,
         messages: [
          { key: rawDoc.value.quartetId + "." + rawDoc.value.nodeId, value: JSON.stringify(rawDoc.value) }
         ],
         compression: CompressionTypes.GZIP,
       })
        await ditto.store.collection(RAW_COLLECTION_NAME).findByID(rawDoc.id).update((mutableDoc) => {
          mutableDoc.at(`${RAW_QUERY_KEY}`).set(true)
        }) 
        // Set synced, and evict
      }
    // Not evicting here, as we're still going to sync the 
    //  await ditto.store.collection(RAW_COLLECTION_NAME).find("synced == true").evict()
    //  console.log("XXX EVICTED!!!")
      signalNext()
    })

  productSubscription = ditto.store.collection(PRODUCT_COLLECTION_NAME).find(`${PRODUCT_QUERY_KEY} == ${PRODUCT_QUERY_VALUE}`).subscribe()
  productLiveQuery = ditto.store
    .collection(PRODUCT_COLLECTION_NAME)
    .find(`${PRODUCT_QUERY_KEY} == ${PRODUCT_QUERY_VALUE}`)
    .observeLocalWithNextSignal(async (docs, event, signalNext) => {
      for (let i = 0; i < docs.length; i++) {
        const productDoc = docs[i]
        Logger.info("PRODUCT DOC to RedPanda: ", productDoc)
        producer.send({
          topic: PRODUCT_TOPIC_NAME,
          messages: [
            { value: JSON.stringify(productDoc.value) }
          ],
        })
        await ditto.store.collection(PRODUCT_COLLECTION_NAME).findByID(productDoc.id).update((mutableDoc) => {
          mutableDoc.at(`${PRODUCT_QUERY_KEY}`).set(true)
        }) 
        // Set synced, and evict
      }
      //ditto.store.collection(PRODUCT_COLLECTION_NAME).find("synced == true").evict()
      signalNext()
    })

  const presenceObserver = ditto.presence.observe((graph) => {
    if (graph.remotePeers.length != 0) {
      graph.remotePeers.forEach((peer) => {
        Logger.info("peer connection: ", peer.deviceName, peer.connections[0].connectionType)
      })
    }
  })

}

main()
