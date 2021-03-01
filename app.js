const express = require('express')
const { writeUserDataToKafka, readMessages } = require('./user.kafka')

const app = express()
const port = 3000


readMessages()


app.get('/send-message', async (req, res) => {
  await writeUserDataToKafka({ email: 'example', isNew: false, message: null })

  res.send('Hello World!')
})

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})