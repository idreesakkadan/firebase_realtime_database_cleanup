// clearing old firebase realtime nodes

import { initializeApp } from "firebase/app"
import { get, getDatabase, limitToFirst, orderByKey, query, ref, startAt, update } from "firebase/database"


const pageSize = 1000
const deleteBeforeMonths = 6
const deleteIfMoreThan = 1000

const firebaseConfig = {
  databaseURL: "https://eventops-prod.firebaseio.com/",
}

const app = initializeApp(firebaseConfig)
const database = getDatabase(app)


const date = new Date()
date.setMonth(date.getMonth() - deleteBeforeMonths)


const processPage = async (snapshotFirstKey) => {
  console.log(`Starting from ${snapshotFirstKey || "beginning"}`)
  const pageQuery = snapshotFirstKey ? query(ref(database), startAt(snapshotFirstKey), limitToFirst(pageSize), orderByKey())
    : query(ref(database), limitToFirst(pageSize), orderByKey())
  const snapshot = await get(pageQuery)
  if (snapshot.exists()) {
    // Check Page
    const snapshotUpdates = {}
    snapshot.forEach(user => {
      let hasMessages = false
      let countMessages = 0
      for (const [messageId, message] of Object.entries(user.val()).reverse()) {
        if (countMessages >= deleteIfMoreThan) {
          console.log(`delete message ${user.key}/${messageId} - Too many`)
          snapshotUpdates[`${user.key}/${messageId}`] = null
        } else if (new Date(message.created_at) <= date) {
          console.log(`delete message ${user.key}/${messageId} - Too old`)
          snapshotUpdates[`${user.key}/${messageId}`] = null
        } else {
          countMessages++
        }
        hasMessages = true
      }
      if (!hasMessages) {
        console.log(`delete user ${user.key} - No messages`)
        snapshotUpdates[user.key] = null
      }
    })
    // Update Page
    update(ref(database), snapshotUpdates)
    // Next Page
    const snapshotLastKey = Object.keys(snapshot.val()).pop()
    if (snapshotLastKey !== snapshotFirstKey) {
      processPage(snapshotLastKey)
    } else {
      console.log("Completed!")
    }
  } else {
    console.log("Completed!")
  }
}
processPage()