import { init, Ditto } from '@dittolive/ditto'
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
  ditto = new Ditto({ type: 'onlinePlayground',
	  appID: APP_ID,
	  token: APP_TOKEN
  })

  ditto.startSync()
  subscription = ditto.store.collection(COLLECTION_NAME).find("isDeleted == false").subscribe()

  const presenceObserver = ditto.presence.observe((graph) => {
    console.log("local peer connections: ", graph.localPeer.connections)
  })
}

main()
