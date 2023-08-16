import { init, Ditto, TransportConfig } from '@dittolive/ditto'
require('dotenv').config()

let ditto

let subscription

let APP_ID = process.env.APP_ID
//let OFFLINE_TOKEN = process.env.OFFLINE_TOKEN
//let SHARED_KEY = process.env.SHARED_KEY
let APP_TOKEN = process.env.APP_TOKEN
let COLLECTION_NAME = process.env.COLLECTION_NAME

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

  ditto.startSync()
  
  subscription = ditto.store.collection(COLLECTION_NAME).find("isDeleted == false").subscribe()

  const presenceObserver = ditto.presence.observe((graph) => {
    console.log("local peer connections: ", graph.localPeer.connections)
  })
}

main()
